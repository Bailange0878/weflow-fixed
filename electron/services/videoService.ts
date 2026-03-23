import { join } from 'path'
import { existsSync, readdirSync, statSync, readFileSync, appendFileSync, mkdirSync } from 'fs'
import { app } from 'electron'
import { ConfigService } from './config'
import { wcdbService } from './wcdbService'

export interface VideoInfo {
    videoUrl?: string       // 视频文件路径（用于 readFile）
    coverUrl?: string       // 封面 data URL
    thumbUrl?: string       // 缩略图 data URL
    exists: boolean
}

class VideoService {
    private configService: ConfigService

    constructor() {
        this.configService = new ConfigService()
    }

    private log(message: string, meta?: Record<string, unknown>): void {
        try {
            const timestamp = new Date().toISOString()
            const metaStr = meta ? ` ${JSON.stringify(meta)}` : ''
            const logDir = join(app.getPath('userData'), 'logs')
            if (!existsSync(logDir)) mkdirSync(logDir, { recursive: true })
            appendFileSync(join(logDir, 'wcdb.log'), `[${timestamp}] [VideoService] ${message}${metaStr}\n`, 'utf8')
        } catch {}
    }

    /**
     * 获取数据库根目录
     */
    private getDbPath(): string {
        return this.configService.get('dbPath') || ''
    }

    /**
     * 获取当前用户的wxid
     */
    private getMyWxid(): string {
        return this.configService.get('myWxid') || ''
    }

    /**
     * 获取缓存目录（解密后的数据库存放位置）
     */
    private getCachePath(): string {
        return this.configService.getCacheBasePath()
    }

    /**
     * 清理 wxid 目录名（去掉后缀）
     */
    private cleanWxid(wxid: string): string {
        const trimmed = wxid.trim()
        if (!trimmed) return trimmed

        if (trimmed.toLowerCase().startsWith('wxid_')) {
            const match = trimmed.match(/^(wxid_[^_]+)/i)
            if (match) return match[1]
            return trimmed
        }

        const suffixMatch = trimmed.match(/^(.+)_([a-zA-Z0-9]{4})$/)
        if (suffixMatch) return suffixMatch[1]

        return trimmed
    }

    /**
     * 从 video_hardlink_info* 表查询视频文件名
     * 使用 wcdbService.execQuery 查询加密的 hardlink.db
     */
    private async queryVideoFileName(md5: string): Promise<string | undefined> {
        const dbPath = this.getDbPath()
        const wxid = this.getMyWxid()
        const cleanedWxid = this.cleanWxid(wxid)

        this.log('queryVideoFileName 开始', { md5, wxid, cleanedWxid, dbPath })

        if (!wxid) {
            this.log('queryVideoFileName: wxid 为空')
            return undefined
        }

        // 使用 wcdbService.execQuery 查询加密的 hardlink.db
        if (dbPath) {
            const dbPathLower = dbPath.toLowerCase()
            const wxidLower = wxid.toLowerCase()
            const cleanedWxidLower = cleanedWxid.toLowerCase()
            const dbPathContainsWxid = dbPathLower.includes(wxidLower) || dbPathLower.includes(cleanedWxidLower)

            const encryptedDbPaths: string[] = []
            if (dbPathContainsWxid) {
                encryptedDbPaths.push(join(dbPath, 'db_storage', 'hardlink', 'hardlink.db'))
            } else {
                encryptedDbPaths.push(join(dbPath, wxid, 'db_storage', 'hardlink', 'hardlink.db'))
                encryptedDbPaths.push(join(dbPath, cleanedWxid, 'db_storage', 'hardlink', 'hardlink.db'))
            }

            for (const p of encryptedDbPaths) {
                if (existsSync(p)) {
                    try {
                        this.log('尝试加密 hardlink.db', { path: p })
                        const escapedMd5 = md5.replace(/'/g, "''")
                        const tableProbe = await wcdbService.execQuery('media', p, "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'video_hardlink_info%'")
                        const tableNames = (tableProbe.success && tableProbe.rows ? tableProbe.rows.map((row: any) => String(row.name || '').trim()).filter(Boolean) : [])
                        const candidates = tableNames.length > 0 ? tableNames.reverse() : ['video_hardlink_info_v4']

                        for (const tableName of candidates) {
                            const sql = `SELECT file_name FROM ${tableName} WHERE md5 = '${escapedMd5}' LIMIT 1`
                            const result = await wcdbService.execQuery('media', p, sql)
                            if (result.success && result.rows && result.rows.length > 0) {
                                const row = result.rows[0]
                                if (row?.file_name) {
                                    const realMd5 = String(row.file_name).replace(/\.[^.]+$/, '')
                                    this.log('加密 hardlink.db 命中', { tableName, file_name: row.file_name, realMd5 })
                                    return realMd5
                                }
                            }
                        }

                        this.log('加密 hardlink.db 未命中', { path: p, tables: candidates })
                    } catch (e) {
                        this.log('加密 hardlink.db 查询失败', { path: p, error: String(e) })
                    }
                } else {
                    this.log('加密 hardlink.db 不存在', { path: p })
                }
            }
        }
        this.log('queryVideoFileName: 所有方法均未找到', { md5 })
        return undefined
    }

    private findExistingFile(candidates: string[]): string | undefined {
        for (const candidate of candidates) {
            if (existsSync(candidate)) return candidate
        }
        return undefined
    }

    private searchVideoFileInDir(dirPath: string, baseNames: string[], exts: string[]): string | undefined {
        if (!existsSync(dirPath)) return undefined
        try {
            const allFiles = readdirSync(dirPath)
            for (const baseName of baseNames) {
                const lowerBase = baseName.toLowerCase()
                const direct = allFiles.find((file) => {
                    const lower = file.toLowerCase()
                    return exts.some((ext) => lower === `${lowerBase}${ext}` || lower === `${lowerBase}_raw${ext}`)
                })
                if (direct) return join(dirPath, direct)

                const fuzzy = allFiles.find((file) => {
                    const lower = file.toLowerCase()
                    return lower.startsWith(lowerBase) && exts.some((ext) => lower.endsWith(ext))
                })
                if (fuzzy) return join(dirPath, fuzzy)
            }
        } catch (e) {
            this.log('searchVideoFileInDir 异常', { dirPath, error: String(e) })
        }
        return undefined
    }

    /**
     * 将文件转换为 data URL
     */
    private fileToDataUrl(filePath: string, mimeType: string): string | undefined {
        try {
            if (!existsSync(filePath)) return undefined
            const buffer = readFileSync(filePath)
            return `data:${mimeType};base64,${buffer.toString('base64')}`
        } catch {
            return undefined
        }
    }

    /**
     * 根据视频MD5获取视频文件信息
     * 视频存放在: {数据库根目录}/{用户wxid}/msg/video/{年月}/
     * 文件命名: {md5}.mp4, {md5}.jpg, {md5}_thumb.jpg
     */
    async getVideoInfo(videoMd5: string): Promise<VideoInfo> {
        const dbPath = this.getDbPath()
        const wxid = this.getMyWxid()

        this.log('getVideoInfo 开始', { videoMd5, dbPath, wxid })

        if (!dbPath || !wxid || !videoMd5) {
            this.log('getVideoInfo: 参数缺失', { dbPath: !!dbPath, wxid: !!wxid, videoMd5: !!videoMd5 })
            return { exists: false }
        }

        // 先尝试从数据库查询真正的视频文件名
        const realVideoMd5 = await this.queryVideoFileName(videoMd5) || videoMd5
        this.log('realVideoMd5', { input: videoMd5, resolved: realVideoMd5, changed: realVideoMd5 !== videoMd5 })

        // 检查 dbPath 是否已经包含 wxid，避免重复拼接
        const dbPathLower = dbPath.toLowerCase()
        const wxidLower = wxid.toLowerCase()
        const cleanedWxid = this.cleanWxid(wxid)

        let videoBaseDir: string
        if (dbPathLower.includes(wxidLower) || dbPathLower.includes(cleanedWxid.toLowerCase())) {
            videoBaseDir = join(dbPath, 'msg', 'video')
        } else {
            videoBaseDir = join(dbPath, wxid, 'msg', 'video')
        }

        this.log('videoBaseDir', { videoBaseDir, exists: existsSync(videoBaseDir) })

        if (!existsSync(videoBaseDir)) {
            this.log('getVideoInfo: videoBaseDir 不存在')
            return { exists: false }
        }

        // 遍历年月目录查找视频文件
        try {
            const allDirs = readdirSync(videoBaseDir)
            const yearMonthDirs = allDirs
                .filter(dir => {
                    const dirPath = join(videoBaseDir, dir)
                    return statSync(dirPath).isDirectory()
                })
                .sort((a, b) => b.localeCompare(a))

            this.log('扫描目录', { dirs: yearMonthDirs })

            const videoExts = ['.mp4', '.mov', '.m4v']
            const baseNames = Array.from(new Set([
                realVideoMd5,
                realVideoMd5.replace(/_raw$/i, ''),
                videoMd5,
                videoMd5.replace(/_raw$/i, '')
            ].filter(Boolean)))

            for (const yearMonth of yearMonthDirs) {
                const dirPath = join(videoBaseDir, yearMonth)
                const videoPath = this.searchVideoFileInDir(dirPath, baseNames, videoExts)

                if (videoPath) {
                    // 封面/缩略图使用不带 _raw 后缀的基础名（自己发的视频文件名带 _raw，但封面不带）
                    const baseMd5 = realVideoMd5.replace(/_raw$/, '')
                    const coverPath = this.findExistingFile([
                        join(dirPath, `${baseMd5}.jpg`),
                        join(dirPath, `${videoMd5}.jpg`)
                    ])
                    const thumbPath = this.findExistingFile([
                        join(dirPath, `${baseMd5}_thumb.jpg`),
                        join(dirPath, `${videoMd5}_thumb.jpg`)
                    ])

                    // 列出同目录下与该 md5 相关的所有文件，帮助排查封面命名
                    const allFiles = readdirSync(dirPath)
                    const relatedFiles = allFiles.filter(f => baseNames.some((name) => f.toLowerCase().startsWith(name.slice(0, 8).toLowerCase())))
                    this.log('找到视频，相关文件列表', {
                        videoPath,
                        coverExists: !!coverPath,
                        thumbExists: !!thumbPath,
                        relatedFiles,
                        coverPath,
                        thumbPath
                    })

                    return {
                        videoUrl: videoPath,
                        coverUrl: coverPath ? this.fileToDataUrl(coverPath, 'image/jpeg') : undefined,
                        thumbUrl: thumbPath ? this.fileToDataUrl(thumbPath, 'image/jpeg') : undefined,
                        exists: true
                    }
                }
            }

            // fallback: 尝试旧 FileStorage/Video 目录
            const dbPathContainsWxid = dbPathLower.includes(wxidLower) || dbPathLower.includes(cleanedWxid.toLowerCase())
            const fileStorageVideoDir = dbPathContainsWxid
                ? join(dbPath, 'FileStorage', 'Video')
                : join(dbPath, wxid, 'FileStorage', 'Video')

            if (existsSync(fileStorageVideoDir)) {
                const fallbackVideoPath = this.searchVideoFileInDir(fileStorageVideoDir, baseNames, videoExts)
                if (fallbackVideoPath) {
                    this.log('在 FileStorage/Video 中找到视频', { fallbackVideoPath })
                    return {
                        videoUrl: fallbackVideoPath,
                        exists: true
                    }
                }
            }

            // 没找到，列出所有目录里的 mp4 文件帮助排查（最多每目录 10 个）
            this.log('未找到视频，开始全目录扫描', {
                lookingForOriginal: `${videoMd5}.mp4`,
                lookingForResolved: `${realVideoMd5}.mp4`,
                hardlinkResolved: realVideoMd5 !== videoMd5
            })
            for (const yearMonth of yearMonthDirs) {
                const dirPath = join(videoBaseDir, yearMonth)
                try {
                    const allFiles = readdirSync(dirPath)
                    const mp4Files = allFiles.filter(f => f.endsWith('.mp4')).slice(0, 10)
                    // 检查原始 md5 是否部分匹配（前8位）
                    const partialMatch = mp4Files.filter(f => f.toLowerCase().startsWith(videoMd5.slice(0, 8).toLowerCase()))
                    this.log(`目录 ${yearMonth} 扫描结果`, {
                        totalFiles: allFiles.length,
                        mp4Count: allFiles.filter(f => f.endsWith('.mp4')).length,
                        sampleMp4: mp4Files,
                        partialMatchByOriginalMd5: partialMatch
                    })
                } catch (e) {
                    this.log(`目录 ${yearMonth} 读取失败`, { error: String(e) })
                }
            }
        } catch (e) {
            this.log('getVideoInfo 遍历出错', { error: String(e) })
        }

        this.log('getVideoInfo: 未找到视频', { videoMd5, realVideoMd5 })
        return { exists: false }
    }

    /**
     * 根据消息内容解析视频MD5
     */
    parseVideoMd5(content: string): string | undefined {
        if (!content) return undefined

        // 打印原始 XML 前 800 字符，帮助排查自己发的视频结构
        this.log('parseVideoMd5 原始内容', { preview: content.slice(0, 800) })

        try {
            // 收集所有 md5 相关属性，方便对比
            const allMd5Attrs: string[] = []
            const md5Regex = /(?:md5|rawmd5|newmd5|originsourcemd5)\s*=\s*['"]([a-fA-F0-9]*)['"]/gi
            let match
            while ((match = md5Regex.exec(content)) !== null) {
                allMd5Attrs.push(match[0])
            }
            this.log('parseVideoMd5 所有 md5 属性', { attrs: allMd5Attrs })

            // 方法1：从 <videomsg md5="..."> 提取（收到的视频）
            const videoMsgMd5Match = /<videomsg[^>]*\smd5\s*=\s*['"]([a-fA-F0-9]+)['"]/i.exec(content)
            if (videoMsgMd5Match) {
                this.log('parseVideoMd5 命中 videomsg md5 属性', { md5: videoMsgMd5Match[1] })
                return videoMsgMd5Match[1].toLowerCase()
            }

            // 方法2：从 <videomsg rawmd5="..."> 提取（自己发的视频，没有 md5 只有 rawmd5）
            const rawMd5Match = /<videomsg[^>]*\srawmd5\s*=\s*['"]([a-fA-F0-9]+)['"]/i.exec(content)
            if (rawMd5Match) {
                this.log('parseVideoMd5 命中 videomsg rawmd5 属性（自发视频）', { rawmd5: rawMd5Match[1] })
                return rawMd5Match[1].toLowerCase()
            }

            // 方法3：任意属性 md5="..."（非 rawmd5/cdnthumbaeskey 等）
            const attrMatch = /(?<![a-z])md5\s*=\s*['"]([a-fA-F0-9]+)['"]/i.exec(content)
            if (attrMatch) {
                this.log('parseVideoMd5 命中通用 md5 属性', { md5: attrMatch[1] })
                return attrMatch[1].toLowerCase()
            }

            // 方法4：<md5>...</md5> 标签
            const md5TagMatch = /<md5>([a-fA-F0-9]+)<\/md5>/i.exec(content)
            if (md5TagMatch) {
                this.log('parseVideoMd5 命中 md5 标签', { md5: md5TagMatch[1] })
                return md5TagMatch[1].toLowerCase()
            }

            // 方法5：兜底取 rawmd5 属性（任意位置）
            const rawMd5Fallback = /\srawmd5\s*=\s*['"]([a-fA-F0-9]+)['"]/i.exec(content)
            if (rawMd5Fallback) {
                this.log('parseVideoMd5 兜底命中 rawmd5', { rawmd5: rawMd5Fallback[1] })
                return rawMd5Fallback[1].toLowerCase()
            }

            this.log('parseVideoMd5 未提取到任何 md5', { contentLength: content.length })
        } catch (e) {
            this.log('parseVideoMd5 异常', { error: String(e) })
        }

        return undefined
    }
}

export const videoService = new VideoService()
