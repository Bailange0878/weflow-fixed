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

interface TimedCacheEntry<T> {
  value: T
  expiresAt: number
}

interface VideoIndexEntry {
  videoPath?: string
  coverPath?: string
  thumbPath?: string
}

class VideoService {
  private configService: ConfigService
  private hardlinkResolveCache = new Map<string, TimedCacheEntry<string | null>>()
  private videoInfoCache = new Map<string, TimedCacheEntry<VideoInfo>>()
  private videoDirIndexCache = new Map<string, TimedCacheEntry<Map<string, VideoIndexEntry>>>()
  private pendingVideoInfo = new Map<string, Promise<VideoInfo>>()
  private readonly hardlinkCacheTtlMs = 10 * 60 * 1000
  private readonly videoInfoCacheTtlMs = 2 * 60 * 1000
  private readonly videoIndexCacheTtlMs = 90 * 1000
  private readonly maxCacheEntries = 2000
  private readonly maxIndexEntries = 6

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
    } catch { }
  }

  private readTimedCache<T>(cache: Map<string, TimedCacheEntry<T>>, key: string): T | undefined {
    const hit = cache.get(key)
    if (!hit) return undefined
    if (hit.expiresAt <= Date.now()) {
      cache.delete(key)
      return undefined
    }
    return hit.value
  }

  private writeTimedCache<T>(
    cache: Map<string, TimedCacheEntry<T>>,
    key: string,
    value: T,
    ttlMs: number,
    maxEntries: number
  ): void {
    cache.set(key, { value, expiresAt: Date.now() + ttlMs })
    if (cache.size <= maxEntries) return

    const now = Date.now()
    for (const [cacheKey, entry] of cache) {
      if (entry.expiresAt <= now) {
        cache.delete(cacheKey)
      }
    }

    while (cache.size > maxEntries) {
      const oldestKey = cache.keys().next().value as string | undefined
      if (!oldestKey) break
      cache.delete(oldestKey)
    }
  }

  private getDbPath(): string {
    return this.configService.get('dbPath') || ''
  }

  private getMyWxid(): string {
    return this.configService.get('myWxid') || ''
  }

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

  private getScopeKey(dbPath: string, wxid: string): string {
    return `${dbPath}::${this.cleanWxid(wxid)}`.toLowerCase()
  }

  private resolveVideoBaseDir(dbPath: string, wxid: string): string {
    const cleanedWxid = this.cleanWxid(wxid)
    const dbPathLower = dbPath.toLowerCase()
    const wxidLower = wxid.toLowerCase()
    const cleanedWxidLower = cleanedWxid.toLowerCase()
    const dbPathContainsWxid = dbPathLower.includes(wxidLower) || dbPathLower.includes(cleanedWxidLower)
    if (dbPathContainsWxid) {
      return join(dbPath, 'msg', 'video')
    }
    return join(dbPath, wxid, 'msg', 'video')
  }

  private getHardlinkDbPaths(dbPath: string, wxid: string, cleanedWxid: string): string[] {
    const dbPathLower = dbPath.toLowerCase()
    const wxidLower = wxid.toLowerCase()
    const cleanedWxidLower = cleanedWxid.toLowerCase()
    const dbPathContainsWxid = dbPathLower.includes(wxidLower) || dbPathLower.includes(cleanedWxidLower)

    if (dbPathContainsWxid) {
      return [join(dbPath, 'db_storage', 'hardlink', 'hardlink.db')]
    }

    return [
      join(dbPath, wxid, 'db_storage', 'hardlink', 'hardlink.db'),
      join(dbPath, cleanedWxid, 'db_storage', 'hardlink', 'hardlink.db')
    ]
  }

  private async resolveVideoHardlinks(
    md5List: string[],
    dbPath: string,
    wxid: string,
    cleanedWxid: string
  ): Promise<Map<string, string>> {
    const scopeKey = this.getScopeKey(dbPath, wxid)
    const normalizedList = Array.from(
      new Set((md5List || []).map((item) => String(item || '').trim().toLowerCase()).filter(Boolean))
    )
    const resolvedMap = new Map<string, string>()
    const unresolvedSet = new Set(normalizedList)

    for (const md5 of normalizedList) {
      const cacheKey = `${scopeKey}|${md5}`
      const cached = this.readTimedCache(this.hardlinkResolveCache, cacheKey)
      if (cached === undefined) continue
      if (cached) resolvedMap.set(md5, cached)
      unresolvedSet.delete(md5)
    }

    if (unresolvedSet.size === 0) return resolvedMap

    const encryptedDbPaths = this.getHardlinkDbPaths(dbPath, wxid, cleanedWxid)
    for (const p of encryptedDbPaths) {
      if (!existsSync(p) || unresolvedSet.size === 0) continue
      const unresolved = Array.from(unresolvedSet)
      try {
        this.log('尝试加密 hardlink.db', { path: p })
        const tableProbe = await wcdbService.execQuery(
          'media',
          p,
          "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'video_hardlink_info%' ORDER BY name DESC"
        )
        const tableNames = (tableProbe.success && tableProbe.rows
          ? tableProbe.rows.map((row: any) => String((row?.name ?? row?.NAME ?? '') || '').trim()).filter(Boolean)
          : [])
        const candidates = tableNames.length > 0 ? tableNames : ['video_hardlink_info_v4']

        for (const inputMd5 of unresolved) {
          const escapedMd5 = inputMd5.replace(/'/g, "''")
          for (const tableName of candidates) {
            try {
              const sql = `SELECT file_name FROM ${tableName} WHERE lower(md5) = lower('${escapedMd5}') LIMIT 1`
              const result = await wcdbService.execQuery('media', p, sql)
              if (result.success && result.rows && result.rows.length > 0) {
                const row = result.rows[0]
                const fileName = String((row?.file_name ?? row?.FILE_NAME ?? '') || '').trim()
                if (!fileName) continue
                const resolvedMd5 = fileName.replace(/\.[^.]+$/, '').toLowerCase()
                const cacheKey = `${scopeKey}|${inputMd5}`
                this.writeTimedCache(this.hardlinkResolveCache, cacheKey, resolvedMd5, this.hardlinkCacheTtlMs, this.maxCacheEntries)
                resolvedMap.set(inputMd5, resolvedMd5)
                unresolvedSet.delete(inputMd5)
                this.log('加密 hardlink.db 命中', { tableName, inputMd5, fileName, resolvedMd5 })
                break
              }
            } catch (e) {
              this.log('加密 hardlink.db 查询失败', { path: p, tableName, md5: inputMd5, error: String(e) })
            }
          }
        }
      } catch (e) {
        this.log('resolveVideoHardlinks 查询失败', { path: p, error: String(e) })
      }
    }

    for (const md5 of unresolvedSet) {
      const cacheKey = `${scopeKey}|${md5}`
      this.writeTimedCache(this.hardlinkResolveCache, cacheKey, null, this.hardlinkCacheTtlMs, this.maxCacheEntries)
    }

    return resolvedMap
  }

  private async queryVideoFileName(md5: string): Promise<string | undefined> {
    const normalizedMd5 = String(md5 || '').trim().toLowerCase()
    const dbPath = this.getDbPath()
    const wxid = this.getMyWxid()
    const cleanedWxid = this.cleanWxid(wxid)

    this.log('queryVideoFileName 开始', { md5: normalizedMd5, wxid, cleanedWxid, dbPath })

    if (!normalizedMd5 || !wxid || !dbPath) {
      this.log('queryVideoFileName: 参数缺失', { hasMd5: !!normalizedMd5, hasWxid: !!wxid, hasDbPath: !!dbPath })
      return undefined
    }

    const resolvedMap = await this.resolveVideoHardlinks([normalizedMd5], dbPath, wxid, cleanedWxid)
    const resolved = resolvedMap.get(normalizedMd5)
    if (resolved) {
      this.log('queryVideoFileName 命中', { input: normalizedMd5, resolved })
      return resolved
    }
    return undefined
  }

  async preloadVideoHardlinkMd5s(md5List: string[]): Promise<void> {
    const dbPath = this.getDbPath()
    const wxid = this.getMyWxid()
    const cleanedWxid = this.cleanWxid(wxid)
    if (!dbPath || !wxid) return
    await this.resolveVideoHardlinks(md5List, dbPath, wxid, cleanedWxid)
  }

  private fileToDataUrl(filePath: string | undefined, mimeType: string): string | undefined {
    try {
      if (!filePath || !existsSync(filePath)) return undefined
      const buffer = readFileSync(filePath)
      return `data:${mimeType};base64,${buffer.toString('base64')}`
    } catch {
      return undefined
    }
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

  private getOrBuildVideoIndex(videoBaseDir: string): Map<string, VideoIndexEntry> {
    const cached = this.readTimedCache(this.videoDirIndexCache, videoBaseDir)
    if (cached) return cached

    const index = new Map<string, VideoIndexEntry>()
    const ensureEntry = (key: string): VideoIndexEntry => {
      let entry = index.get(key)
      if (!entry) {
        entry = {}
        index.set(key, entry)
      }
      return entry
    }

    try {
      const yearMonthDirs = readdirSync(videoBaseDir)
        .filter((dir) => {
          const dirPath = join(videoBaseDir, dir)
          try {
            return statSync(dirPath).isDirectory()
          } catch {
            return false
          }
        })
        .sort((a, b) => b.localeCompare(a))

      for (const yearMonth of yearMonthDirs) {
        const dirPath = join(videoBaseDir, yearMonth)
        let files: string[] = []
        try {
          files = readdirSync(dirPath)
        } catch {
          continue
        }

        for (const file of files) {
          const lower = file.toLowerCase()
          const fullPath = join(dirPath, file)

          if (lower.endsWith('.mp4') || lower.endsWith('.mov') || lower.endsWith('.m4v')) {
            const md5 = lower.replace(/\.[^.]+$/, '')
            const entry = ensureEntry(md5)
            if (!entry.videoPath) entry.videoPath = fullPath
            if (md5.endsWith('_raw')) {
              const baseMd5 = md5.replace(/_raw$/, '')
              const baseEntry = ensureEntry(baseMd5)
              if (!baseEntry.videoPath) baseEntry.videoPath = fullPath
            }
            continue
          }

          if (!lower.endsWith('.jpg')) continue
          const jpgBase = lower.slice(0, -4)
          if (jpgBase.endsWith('_thumb')) {
            const baseMd5 = jpgBase.slice(0, -6)
            const entry = ensureEntry(baseMd5)
            if (!entry.thumbPath) entry.thumbPath = fullPath
          } else {
            const entry = ensureEntry(jpgBase)
            if (!entry.coverPath) entry.coverPath = fullPath
          }
        }
      }

      for (const [key, entry] of index) {
        if (!key.endsWith('_raw')) continue
        const baseKey = key.replace(/_raw$/, '')
        const baseEntry = index.get(baseKey)
        if (!baseEntry) continue
        if (!entry.coverPath) entry.coverPath = baseEntry.coverPath
        if (!entry.thumbPath) entry.thumbPath = baseEntry.thumbPath
      }
    } catch (e) {
      this.log('构建视频索引失败', { videoBaseDir, error: String(e) })
    }

    this.writeTimedCache(
      this.videoDirIndexCache,
      videoBaseDir,
      index,
      this.videoIndexCacheTtlMs,
      this.maxIndexEntries
    )
    return index
  }

  private getVideoInfoFromIndex(index: Map<string, VideoIndexEntry>, md5: string, includePoster = true): VideoInfo | null {
    const normalizedMd5 = String(md5 || '').trim().toLowerCase()
    if (!normalizedMd5) return null

    const candidates = [normalizedMd5]
    const baseMd5 = normalizedMd5.replace(/_raw$/, '')
    if (baseMd5 !== normalizedMd5) {
      candidates.push(baseMd5)
    } else {
      candidates.push(`${normalizedMd5}_raw`)
    }

    for (const key of candidates) {
      const entry = index.get(key)
      if (!entry?.videoPath) continue
      if (!existsSync(entry.videoPath)) continue
      if (!includePoster) {
        return {
          videoUrl: entry.videoPath,
          exists: true
        }
      }
      return {
        videoUrl: entry.videoPath,
        coverUrl: this.fileToDataUrl(entry.coverPath, 'image/jpeg'),
        thumbUrl: this.fileToDataUrl(entry.thumbPath, 'image/jpeg'),
        exists: true
      }
    }

    return null
  }

  private fallbackScanVideo(videoBaseDir: string, realVideoMd5: string, includePoster = true): VideoInfo | null {
    try {
      const yearMonthDirs = readdirSync(videoBaseDir)
        .filter((dir) => {
          const dirPath = join(videoBaseDir, dir)
          try {
            return statSync(dirPath).isDirectory()
          } catch {
            return false
          }
        })
        .sort((a, b) => b.localeCompare(a))

      for (const yearMonth of yearMonthDirs) {
        const dirPath = join(videoBaseDir, yearMonth)
        const videoExts = ['.mp4', '.mov', '.m4v']
        const baseNames = Array.from(new Set([
          realVideoMd5,
          realVideoMd5.replace(/_raw$/i, '')
        ].filter(Boolean)))
        const videoPath = this.searchVideoFileInDir(dirPath, baseNames, videoExts)
        if (!videoPath) continue
        if (!includePoster) {
          return {
            videoUrl: videoPath,
            exists: true
          }
        }
        const baseMd5 = realVideoMd5.replace(/_raw$/, '')
        const coverPath = this.findExistingFile([
          join(dirPath, `${baseMd5}.jpg`),
          join(dirPath, `${realVideoMd5}.jpg`)
        ])
        const thumbPath = this.findExistingFile([
          join(dirPath, `${baseMd5}_thumb.jpg`),
          join(dirPath, `${realVideoMd5}_thumb.jpg`)
        ])
        return {
          videoUrl: videoPath,
          coverUrl: this.fileToDataUrl(coverPath, 'image/jpeg'),
          thumbUrl: this.fileToDataUrl(thumbPath, 'image/jpeg'),
          exists: true
        }
      }
    } catch (e) {
      this.log('fallback 扫描视频目录失败', { error: String(e) })
    }
    return null
  }

  async getVideoInfo(videoMd5: string, options?: { includePoster?: boolean }): Promise<VideoInfo> {
    const normalizedMd5 = String(videoMd5 || '').trim().toLowerCase()
    const includePoster = options?.includePoster !== false
    const dbPath = this.getDbPath()
    const wxid = this.getMyWxid()

    this.log('getVideoInfo 开始', { videoMd5: normalizedMd5, dbPath, wxid })

    if (!dbPath || !wxid || !normalizedMd5) {
      this.log('getVideoInfo: 参数缺失', { hasDbPath: !!dbPath, hasWxid: !!wxid, hasVideoMd5: !!normalizedMd5 })
      return { exists: false }
    }

    const scopeKey = this.getScopeKey(dbPath, wxid)
    const cacheKey = `${scopeKey}|${normalizedMd5}|poster=${includePoster ? 1 : 0}`

    const cachedInfo = this.readTimedCache(this.videoInfoCache, cacheKey)
    if (cachedInfo) return cachedInfo

    const pending = this.pendingVideoInfo.get(cacheKey)
    if (pending) return pending

    const task = (async (): Promise<VideoInfo> => {
      const realVideoMd5 = await this.queryVideoFileName(normalizedMd5) || normalizedMd5
      const videoBaseDir = this.resolveVideoBaseDir(dbPath, wxid)

      if (existsSync(videoBaseDir)) {
        const index = this.getOrBuildVideoIndex(videoBaseDir)
        const indexed = this.getVideoInfoFromIndex(index, realVideoMd5, includePoster)
        if (indexed) {
          this.writeTimedCache(this.videoInfoCache, cacheKey, indexed, this.videoInfoCacheTtlMs, this.maxCacheEntries)
          return indexed
        }

        const fallback = this.fallbackScanVideo(videoBaseDir, realVideoMd5, includePoster)
        if (fallback) {
          this.writeTimedCache(this.videoInfoCache, cacheKey, fallback, this.videoInfoCacheTtlMs, this.maxCacheEntries)
          return fallback
        }
      }

      const cleanedWxid = this.cleanWxid(wxid)
      const dbPathLower = dbPath.toLowerCase()
      const wxidLower = wxid.toLowerCase()
      const cleanedWxidLower = cleanedWxid.toLowerCase()
      const dbPathContainsWxid = dbPathLower.includes(wxidLower) || dbPathLower.includes(cleanedWxidLower)
      const fileStorageVideoDir = dbPathContainsWxid
        ? join(dbPath, 'FileStorage', 'Video')
        : join(dbPath, wxid, 'FileStorage', 'Video')

      if (existsSync(fileStorageVideoDir)) {
        const videoExts = ['.mp4', '.mov', '.m4v']
        const baseNames = Array.from(new Set([
          realVideoMd5,
          realVideoMd5.replace(/_raw$/i, ''),
          normalizedMd5,
          normalizedMd5.replace(/_raw$/i, '')
        ].filter(Boolean)))
        const fallbackVideoPath = this.searchVideoFileInDir(fileStorageVideoDir, baseNames, videoExts)
        if (fallbackVideoPath) {
          this.log('在 FileStorage/Video 中找到视频', { fallbackVideoPath })
          const hit = {
            videoUrl: fallbackVideoPath,
            exists: true
          }
          this.writeTimedCache(this.videoInfoCache, cacheKey, hit, this.videoInfoCacheTtlMs, this.maxCacheEntries)
          return hit
        }
      }

      const miss = { exists: false }
      this.writeTimedCache(this.videoInfoCache, cacheKey, miss, this.videoInfoCacheTtlMs, this.maxCacheEntries)
      this.log('getVideoInfo: 未找到视频', { inputMd5: normalizedMd5, resolvedMd5: realVideoMd5 })
      return miss
    })()

    this.pendingVideoInfo.set(cacheKey, task)
    try {
      return await task
    } finally {
      this.pendingVideoInfo.delete(cacheKey)
    }
  }

  parseVideoMd5(content: string): string | undefined {
    if (!content) return undefined

    this.log('parseVideoMd5 原始内容', { preview: content.slice(0, 800) })

    try {
      const allMd5Attrs: string[] = []
      const md5Regex = /(?:md5|rawmd5|newmd5|originsourcemd5)\s*=\s*['"]([a-fA-F0-9]*)['"]/gi
      let match
      while ((match = md5Regex.exec(content)) !== null) {
        allMd5Attrs.push(match[0])
      }
      this.log('parseVideoMd5 所有 md5 属性', { attrs: allMd5Attrs })

      const videoMsgMd5Match = /<videomsg[^>]*\smd5\s*=\s*['"]([a-fA-F0-9]+)['"]/i.exec(content)
      if (videoMsgMd5Match) {
        this.log('parseVideoMd5 命中 videomsg md5 属性', { md5: videoMsgMd5Match[1] })
        return videoMsgMd5Match[1].toLowerCase()
      }

      const rawMd5Match = /<videomsg[^>]*\srawmd5\s*=\s*['"]([a-fA-F0-9]+)['"]/i.exec(content)
      if (rawMd5Match) {
        this.log('parseVideoMd5 命中 videomsg rawmd5 属性（自发视频）', { rawmd5: rawMd5Match[1] })
        return rawMd5Match[1].toLowerCase()
      }

      const attrMatch = /(?<![a-z])md5\s*=\s*['"]([a-fA-F0-9]+)['"]/i.exec(content)
      if (attrMatch) {
        this.log('parseVideoMd5 命中通用 md5 属性', { md5: attrMatch[1] })
        return attrMatch[1].toLowerCase()
      }

      const md5TagMatch = /<md5>([a-fA-F0-9]+)<\/md5>/i.exec(content)
      if (md5TagMatch) {
        this.log('parseVideoMd5 命中 md5 标签', { md5: md5TagMatch[1] })
        return md5TagMatch[1].toLowerCase()
      }

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
