import { join, basename, extname, dirname } from 'path'
import { existsSync, readdirSync, statSync, readFileSync, appendFileSync, mkdirSync, writeFileSync } from 'fs'
import { app } from 'electron'
import { ConfigService } from './config'
import Database from 'better-sqlite3'
import { wcdbService } from './wcdbService'
import crypto from 'crypto'

export interface VideoInfo {
    videoUrl?: string       // 视频文件路径
    coverUrl?: string       // 封面 data URL
    thumbUrl?: string       // 缩略图 data URL
    exists: boolean
}

class VideoService {
    private configService: ConfigService
    private resolvedCache = new Map<string, string>() // md5 -> localPath

    constructor() {
        this.configService = new ConfigService()
    }

    private logInfo(message: string, meta?: Record<string, unknown>): void {
        if (!this.configService.get('logEnabled')) return
        const timestamp = new Date().toISOString()
        const metaStr = meta ? ` ${JSON.stringify(meta)}` : ''
        const logLine = `[${timestamp}] [VideoService] ${message}${metaStr}\n`
        this.writeLog(logLine)
    }

    private logError(message: string, error?: unknown, meta?: Record<string, unknown>): void {
        if (!this.configService.get('logEnabled')) return
        const timestamp = new Date().toISOString()
        const errorStr = error ? ` Error: ${String(error)}` : ''
        const metaStr = meta ? ` ${JSON.stringify(meta)}` : ''
        const logLine = `[${timestamp}] [VideoService] ERROR: ${message}${errorStr}${metaStr}\n`
        console.error(`[VideoService] ${message}`, error, meta)
        this.writeLog(logLine)
    }

    private writeLog(line: string): void {
        try {
            const logDir = join(app.getPath('userData'), 'logs')
            if (!existsSync(logDir)) {
                mkdirSync(logDir, { recursive: true })
            }
            appendFileSync(join(logDir, 'wcdb.log'), line, { encoding: 'utf8' })
        } catch (err) {
            console.error('写入日志失败:', err)
        }
    }

    private getDbPath(): string {
        return this.configService.get('dbPath') || ''
    }

    private getMyWxid(): string {
        return this.configService.get('myWxid') || ''
    }

    private getCacheBasePath(): string {
        return this.configService.getCacheBasePath()
    }

    private cleanWxid(wxid: string): string {
        const trimmed = wxid.trim()
        if (!trimmed) return trimmed
        if (trimmed.toLowerCase().startsWith('wxid_')) {
            const match = trimmed.match(/^(wxid_[^_]+)/i)
            if (match) return match[1]
        }
        const suffixMatch = trimmed.match(/^(.+)_([a-zA-Z0-9]{4})$/)
        if (suffixMatch) return suffixMatch[1]
        return trimmed
    }

    private resolveAccountDir(dbPath: string, wxid: string): string | null {
        if (!dbPath || !wxid) return null
        const cleanedWxid = this.cleanWxid(wxid).toLowerCase()
        const normalized = dbPath.replace(/[\\/]+$/, '')
        const candidates: { path: string; mtime: number }[] = []

        const checkDir = (p: string) => {
            if (existsSync(p) && (existsSync(join(p, 'db_storage')) || existsSync(join(p, 'msg', 'video')) || existsSync(join(p, 'msg', 'attach')))) {
                candidates.push({ path: p, mtime: this.getDirMtime(p) })
            }
        }

        checkDir(join(normalized, wxid))
        checkDir(join(normalized, cleanedWxid))
        checkDir(normalized)

        try {
            if (existsSync(normalized) && statSync(normalized).isDirectory()) {
                const entries = readdirSync(normalized)
                for (const entry of entries) {
                    const entryPath = join(normalized, entry)
                    try {
                        if (!statSync(entryPath).isDirectory()) continue
                    } catch { continue }
                    const lowerEntry = entry.toLowerCase()
                    if (lowerEntry === cleanedWxid || lowerEntry.startsWith(`${cleanedWxid}_`)) {
                        checkDir(entryPath)
                    }
                }
            }
        } catch { }

        if (candidates.length === 0) return null
        candidates.sort((a, b) => b.mtime - a.mtime)
        return candidates[0].path
    }

    private getDirMtime(dirPath: string): number {
        try {
            let mtime = statSync(dirPath).mtimeMs
            const subs = ['db_storage', 'msg/video', 'msg/attach']
            for (const sub of subs) {
                const p = join(dirPath, sub)
                if (existsSync(p)) mtime = Math.max(mtime, statSync(p).mtimeMs)
            }
            return mtime
        } catch { return 0 }
    }

    private async ensureWcdbReady(): Promise<boolean> {
        if (wcdbService.isReady()) return true
        const dbPath = this.configService.get('dbPath')
        const decryptKey = this.configService.get('decryptKey')
        const wxid = this.configService.get('myWxid')
        if (!dbPath || !decryptKey || !wxid) return false
        const cleanedWxid = this.cleanWxid(wxid)
        return await wcdbService.open(dbPath, decryptKey, cleanedWxid)
    }

    /**
     * 计算会话哈希（对应磁盘目录名）
     */
    private md5Hash(text: string): string {
        return crypto.createHash('md5').update(text).digest('hex')
    }

    private async resolveHardlinkPath(accountDir: string, md5: string): Promise<string | null> {
        const dbPath = join(accountDir, 'db_storage', 'hardlink', 'hardlink.db')
        if (!existsSync(dbPath)) {
            this.logInfo('hardlink.db 不存在', { dbPath })
            return null
        }

        try {
            const ready = await this.ensureWcdbReady()
            if (!ready) return null

            const tableResult = await wcdbService.execQuery('media', dbPath, 
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'video_hardlink_info%' ORDER BY name DESC LIMIT 1")
            
            if (!tableResult.success || !tableResult.rows?.length) return null
            const tableName = tableResult.rows[0].name

            const escapedMd5 = md5.replace(/'/g, "''")
            const rowResult = await wcdbService.execQuery('media', dbPath, 
                `SELECT dir1, dir2, file_name FROM ${tableName} WHERE lower(md5) = lower('${escapedMd5}') LIMIT 1`)
            
            if (!rowResult.success || !rowResult.rows?.length) return null
            
            const row = rowResult.rows[0]
            const dir1 = row.dir1 ?? row.DIR1
            const dir2 = row.dir2 ?? row.DIR2
            const file_name = row.file_name ?? row.fileName ?? row.FILE_NAME

            if (dir1 === undefined || dir2 === undefined || !file_name) return null

            const dirTableResult = await wcdbService.execQuery('media', dbPath, 
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'dir2id%' LIMIT 1")
            if (!dirTableResult.success || !dirTableResult.rows?.length) return null
            const dirTable = dirTableResult.rows[0].name

            const getDirName = async (id: number) => {
                const res = await wcdbService.execQuery('media', dbPath, `SELECT username FROM ${dirTable} WHERE rowid = ${id} LIMIT 1`)
                return res.success && res.rows?.length ? String(res.rows[0].username) : null
            }

            const dir1Name = await getDirName(Number(dir1))
            const dir2Name = await getDirName(Number(dir2))
            if (!dir1Name || !dir2Name) return null

            const candidates = [
                join(accountDir, 'msg', 'attach', dir1Name, dir2Name, 'Video', file_name),
                join(accountDir, 'msg', 'attach', dir1Name, dir2Name, file_name),
                join(accountDir, 'msg', 'video', dir2Name, file_name)
            ]

            for (const p of candidates) {
                if (existsSync(p)) {
                    this.logInfo('hardlink 命中', { path: p })
                    return p
                }
            }
        } catch (e) {
            this.logError('resolveHardlinkPath 异常', e)
        }
        return null
    }

    private async searchVideoFile(accountDir: string, md5: string, sessionId?: string): Promise<string | null> {
        const lowerMd5 = md5.toLowerCase()
        
        // 策略 1: 基于 sessionId 哈希的精准搜索 (XWeChat 核心逻辑)
        if (sessionId) {
            const sessHash = this.md5Hash(sessionId)
            const attachRoot = join(accountDir, 'msg', 'attach', sessHash)
            if (existsSync(attachRoot)) {
                try {
                    const monthDirs = readdirSync(attachRoot).filter(d => /^\d{4}-\d{2}$/.test(d))
                    for (const m of monthDirs) {
                        const videoDir = join(attachRoot, m, 'Video')
                        if (existsSync(videoDir)) {
                            // 尝试精确名和带数字后缀的名
                            const files = readdirSync(videoDir)
                            const match = files.find(f => f.toLowerCase().startsWith(lowerMd5) && f.toLowerCase().endsWith('.mp4'))
                            if (match) return join(videoDir, match)
                        }
                    }
                } catch { }
            }
        }

        // 策略 2: 概率搜索所有 session 目录 (针对最近 3 个月)
        const attachRoot = join(accountDir, 'msg', 'attach')
        if (existsSync(attachRoot)) {
            try {
                const sessionDirs = readdirSync(attachRoot).filter(d => d.length === 32)
                const now = new Date()
                const months = []
                for (let i = 0; i < 3; i++) {
                    const d = new Date(now.getFullYear(), now.getMonth() - i, 1)
                    months.push(`${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`)
                }

                for (const sess of sessionDirs) {
                    for (const month of months) {
                        const videoDir = join(attachRoot, sess, month, 'Video')
                        if (existsSync(videoDir)) {
                            const files = readdirSync(videoDir)
                            const match = files.find(f => f.toLowerCase().startsWith(lowerMd5) && f.toLowerCase().endsWith('.mp4'))
                            if (match) return join(videoDir, match)
                        }
                    }
                }
            } catch { }
        }

        // 策略 3: 传统 msg/video 目录
        const videoRoot = join(accountDir, 'msg', 'video')
        if (existsSync(videoRoot)) {
            try {
                const monthDirs = readdirSync(videoRoot).sort().reverse()
                for (const m of monthDirs) {
                    const dirPath = join(videoRoot, m)
                    const files = readdirSync(dirPath)
                    const match = files.find(f => f.toLowerCase().startsWith(lowerMd5) && f.toLowerCase().endsWith('.mp4'))
                    if (match) return join(dirPath, match)
                }
            } catch { }
        }

        return null
    }

    private getXorKey(): number | undefined {
        const raw = this.configService.get('imageXorKey')
        if (typeof raw === 'number') return raw
        if (typeof raw === 'string') {
            const t = raw.trim()
            return t.toLowerCase().startsWith('0x') ? parseInt(t, 16) : parseInt(t, 10)
        }
        return undefined
    }

    private isEncrypted(buffer: Buffer, xorKey: number, type: 'video' | 'image'): boolean {
        if (buffer.length < 8) return false
        const first = buffer[0] ^ xorKey
        const second = buffer[1] ^ xorKey
        
        if (type === 'image') {
            return (first === 0xFF && second === 0xD8) || (first === 0x89 && second === 0x50) || (first === 0x47 && second === 0x49)
        } else {
            // MP4 头部通常包含 'ftyp'
            const f = buffer[4] ^ xorKey
            const t = buffer[5] ^ xorKey
            const y = buffer[6] ^ xorKey
            const p = buffer[7] ^ xorKey
            return (f === 0x66 && t === 0x74 && y === 0x79 && p === 0x70) || // 'ftyp'
                   (buffer[0] ^ xorKey) === 0x00 && (buffer[1] ^ xorKey) === 0x00 // 一些 mp4 以 00 00 开头
        }
    }

    private filePathToUrl(filePath: string): string {
        try {
            const { pathToFileURL } = require('url')
            const url = pathToFileURL(filePath).toString()
            const s = statSync(filePath)
            return `${url}?v=${Math.floor(s.mtimeMs)}`
        } catch {
            return `file:///${filePath.replace(/\\/g, '/')}`
        }
    }

    private handleFile(filePath: string, type: 'video' | 'image', sessionId?: string): string | undefined {
        if (!existsSync(filePath)) return undefined
        const xorKey = this.getXorKey()
        
        try {
            const buffer = readFileSync(filePath)
            const isEnc = xorKey !== undefined && !Number.isNaN(xorKey) && this.isEncrypted(buffer, xorKey, type)

            if (isEnc) {
                const decrypted = Buffer.alloc(buffer.length)
                for (let i = 0; i < buffer.length; i++) decrypted[i] = buffer[i] ^ xorKey!
                
                if (type === 'image') {
                    return `data:image/jpeg;base64,${decrypted.toString('base64')}`
                } else {
                    const cacheDir = join(this.getCacheBasePath(), 'Videos', this.cleanWxid(sessionId || 'unknown'))
                    if (!existsSync(cacheDir)) mkdirSync(cacheDir, { recursive: true })
                    const outPath = join(cacheDir, `${basename(filePath)}`)
                    if (!existsSync(outPath) || statSync(outPath).size !== decrypted.length) {
                        writeFileSync(outPath, decrypted)
                    }
                    return this.filePathToUrl(outPath)
                }
            }

            if (type === 'image') {
                return `data:image/jpeg;base64,${buffer.toString('base64')}`
            }
            return this.filePathToUrl(filePath)
        } catch (e) {
            this.logError(`处理${type}文件异常: ${filePath}`, e)
            return type === 'image' ? undefined : this.filePathToUrl(filePath)
        }
    }

    async getVideoInfo(videoMd5: string, sessionId?: string): Promise<VideoInfo> {
        this.logInfo('获取视频信息', { videoMd5, sessionId })
        const dbPath = this.getDbPath()
        const wxid = this.getMyWxid()
        if (!dbPath || !wxid || !videoMd5) return { exists: false }

        const accountDir = this.resolveAccountDir(dbPath, wxid)
        if (!accountDir) {
            this.logError('未找到账号目录', undefined, { dbPath, wxid })
            return { exists: false }
        }

        // 1. 通过 hardlink 映射
        let videoPath = await this.resolveHardlinkPath(accountDir, videoMd5)
        
        // 2. 启发式搜索
        if (!videoPath) {
            videoPath = await this.searchVideoFile(accountDir, videoMd5, sessionId)
        }

        if (videoPath && existsSync(videoPath)) {
            this.logInfo('定位成功', { videoPath })
            const base = videoPath.slice(0, -4)
            const coverPath = `${base}.jpg`
            const thumbPath = `${base}_thumb.jpg`

            return {
                videoUrl: this.handleFile(videoPath, 'video', sessionId),
                coverUrl: this.handleFile(coverPath, 'image', sessionId),
                thumbUrl: this.handleFile(thumbPath, 'image', sessionId),
                exists: true
            }
        }

        this.logInfo('定位失败', { videoMd5 })
        return { exists: false }
    }

    parseVideoMd5(content: string): string | undefined {
        if (!content) return undefined
        try {
            const m = /<videomsg[^>]*\smd5\s*=\s*['"]([a-fA-F0-9]+)['"]/i.exec(content) || 
                    /\smd5\s*=\s*['"]([a-fA-F0-9]+)['"]/i.exec(content) ||
                    /<md5>([a-fA-F0-9]+)<\/md5>/i.exec(content)
            return m ? m[1].toLowerCase() : undefined
        } catch { return undefined }
    }
}

export const videoService = new VideoService()
