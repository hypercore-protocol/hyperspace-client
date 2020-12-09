const { EventEmitter } = require('events')
const maybe = require('call-me-maybe')
const codecs = require('codecs')
const inspect = require('inspect-custom-symbol')
const FreeMap = require('freemap')
const { WriteStream, ReadStream } = require('hypercore-streams')
const PROMISES = Symbol.for('hypercore.promises')

const { NanoresourcePromise: Nanoresource } = require('nanoresource-promise/emitter')
const HRPC = require('@hyperspace/rpc')
const getNetworkOptions = require('@hyperspace/rpc/socket')
const net = require('net')

class Sessions {
  constructor () {
    this._cores = new FreeMap()
    this._resourceCounter = 0
  }

  create (remoteCore) {
    return this._cores.add(remoteCore)
  }

  createResourceId () {
    return this._resourceCounter++
  }

  delete (id) {
    this._cores.free(id)
  }

  get (id) {
    return this._cores.get(id)
  }
}

class RemoteCorestore extends EventEmitter {
  constructor (opts = {}) {
    super()

    this.name = opts.name || null
    this._client = opts.client
    this._sessions = opts.sessions || new Sessions()
    this._feeds = new Map()

    this._client.hypercore.onRequest(this, {
      onAppend ({ id, length, byteLength }) {
        const remoteCore = this._sessions.get(id)
        if (!remoteCore) throw new Error('Invalid RemoteHypercore ID.')
        remoteCore._onappend({ length, byteLength })
      },
      onClose ({ id }) {
        const remoteCore = this._sessions.get(id)
        if (!remoteCore) throw new Error('Invalid RemoteHypercore ID.')
        remoteCore.close(() => {}) // no unhandled rejects
      },
      onPeerOpen ({ id, peer }) {
        const remoteCore = this._sessions.get(id)
        if (!remoteCore) throw new Error('Invalid RemoteHypercore ID.')
        remoteCore._onpeeropen(peer)
      },
      onPeerRemove ({ id, peer }) {
        const remoteCore = this._sessions.get(id)
        if (!remoteCore) throw new Error('Invalid RemoteHypercore ID.')
        remoteCore._onpeerremove(peer)
      },
      onExtension ({ id, resourceId, remotePublicKey, data }) {
        const remoteCore = this._sessions.get(id)
        if (!remoteCore) throw new Error('Invalid RemoteHypercore ID.')
        remoteCore._onextension({ resourceId, remotePublicKey, data })
      },
      onWait ({ id, onWaitId, seq }) {
        const remoteCore = this._sessions.get(id)
        if (!remoteCore) throw new Error('Invalid RemoteHypercore ID.')
        remoteCore._onwait(onWaitId, seq)
      },
      onDownload ({ id, seq, byteLength }) {
        const remoteCore = this._sessions.get(id)
        if (!remoteCore) throw new Error('Invalid RemoteHypercore ID.')
        remoteCore._ondownload({ seq, byteLength })
      },
      onUpload ({ id, seq, byteLength }) {
        const remoteCore = this._sessions.get(id)
        if (!remoteCore) throw new Error('Invalid RemoteHypercore ID.')
        remoteCore._onupload({ seq, byteLength })
      }
    })
    this._client.corestore.onRequest(this, {
      onFeed ({ key }) {
        return this._onfeed(key)
      }
    })
  }

  // Events

  _onfeed (key) {
    if (!this.listenerCount('feed')) return
    this.emit('feed', this.get(key, { weak: true, lazy: true }))
  }

  // Public Methods

  replicate () {
    throw new Error('Cannot call replicate on a RemoteCorestore')
  }

  default (opts = {}) {
    return this.get(opts.key, { name: this.name })
  }

  get (key, opts = {}) {
    if (key && typeof key !== 'string' && !Buffer.isBuffer(key)) {
      opts = key
      key = opts.key
    }
    if (typeof key === 'string') key = Buffer.from(key, 'hex')

    let hex = key && key.toString('hex')
    if (hex && this._feeds.has(hex)) return this._feeds.get(hex)

    const feed = new RemoteHypercore(this._client, this._sessions, key, opts)

    if (hex) {
      this._feeds.set(hex, feed)
    } else {
      feed.on('ready', () => {
        hex = feed.key.toString('hex')
        if (!this._feeds.has(hex)) this._feeds.set(hex, feed)
      })
    }

    feed.on('close', () => {
      if (hex && this._feeds.get(hex) === feed) this._feeds.delete(hex)
    })

    return feed
  }

  namespace (name) {
    return new this.constructor({
      client: this._client,
      sessions: this._sessions,
      name: name || randomNamespace()
    })
  }

  ready (cb) {
    if (cb) process.nextTick(cb, null)
  }

  async _closeAll () {
    const proms = []
    for (const [k, feed] of this._feeds) {
      this._feeds.delete(k)
      proms.push(feed.close())
    }

    try {
      await Promise.all(proms)
    } catch (err) {
      await Promise.allSettled(proms)
      throw err
    }
  }

  close (cb) {
    return maybeOptional(cb, this._closeAll())
  }
}

class RemoteNetworker extends EventEmitter {
  constructor (opts) {
    super()
    this._client = opts.client
    this._sessions = opts.sessions
    this._extensions = new Map()

    this.peers = null
    this.publicKey = null

    this._client.network.onRequest(this, {
      onPeerAdd: this._onpeeradd.bind(this),
      onPeerRemove: this._onpeerremove.bind(this),
      onExtension: this._onextension.bind(this)
    })

    this.ready().catch(noop)
  }

  // Event Handlers

  _onpeeradd ({ peer }) {
    this.peers.push(peer)
    this.emit('peer-open', peer)
    this.emit('peer-add', peer)
  }

  _onpeerremove ({ peer }) {
    const idx = this._indexOfPeer(peer.remotePublicKey)
    if (idx === -1) return
    this.peers[idx] = this.peers[this.peers.length - 1]
    this.peers.pop()
    this.emit('peer-remove', peer)
  }

  _onextension ({ resourceId, remotePublicKey, data }) {
    const idx = this._indexOfPeer(remotePublicKey)
    if (idx === -1) return
    const remotePeer = this.peers[idx]
    const ext = this._extensions.get(resourceId)
    if (ext.destroyed) return
    ext.onmessage(data, remotePeer)
  }

  // Private Methods

  _indexOfPeer (remotePublicKey) {
    for (let i = 0; i < this.peers.length; i++) {
      if (remotePublicKey.equals(this.peers[i].remotePublicKey)) return i
    }
    return -1
  }

  async _open () {
    if (this.peers) return null
    const rsp = await this._client.network.open()
    this.peers = rsp.peers
    this.keyPair = {
      publicKey: rsp.publicKey,
      privateKey: null
    }
  }

  async _configure (discoveryKey, opts) {
    if (typeof discoveryKey === 'object' && !Buffer.isBuffer(discoveryKey)) {
      const core = discoveryKey
      if (!core.discoveryKey) await core.ready()
      discoveryKey = core.discoveryKey
    }
    return this._client.network.configure({
      configuration: {
        discoveryKey,
        announce: opts.announce,
        lookup: opts.lookup,
        remember: opts.remember
      },
      flush: opts.flush,
      copyFrom: opts.copyFrom,
      overwrite: opts.overwrite
    })
  }

  // Public Methods

  ready (cb) {
    return maybe(cb, this._open())
  }

  configure (discoveryKey, opts = {}, cb) {
    const configureProm = this._configure(discoveryKey, opts)
    maybeOptional(cb, configureProm)
    return configureProm
  }

  async status (discoveryKey, cb) {
    return maybe(cb, (async () => {
      const rsp = await this._client.network.status({
        discoveryKey
      })
      return rsp.status
    })())
  }

  async allStatuses (cb) {
    return maybe(cb, (async () => {
      const rsp = await this._client.network.allStatuses()
      return rsp.statuses
    })())
  }

  registerExtension (name, opts) {
    const ext = new RemoteNetworkerExtension(this, name, opts)
    this._extensions.set(ext.resourceId, ext)
    return ext
  }
}

class RemoteNetworkerExtension {
  constructor (networker, name, opts = {}) {
    if (typeof name === 'object') {
      opts = name
      name = opts.name
    }
    this.networker = networker
    this.resourceId = networker._sessions.createResourceId()
    this.name = name
    this.encoding = codecs((opts && opts.encoding) || 'binary')
    this.destroyed = false

    this.onerror = opts.onerror || noop
    this.onmessage = noop
    if (opts.onmessage) {
      this.onmessage = (message, peer) => {
        try {
          message = this.encoding.decode(message)
        } catch (err) {
          return this.onerror(err)
        }
        return opts.onmessage(message, peer)
      }
    }

    this.networker._client.network.registerExtensionNoReply({
      id: 0,
      resourceId: this.resourceId,
      name: this.name
    })
  }

  broadcast (message) {
    if (this.destroyed) return
    const buf = this.encoding.encode(message)
    this.networker._client.network.sendExtensionNoReply({
      id: 0,
      resourceId: this.resourceId,
      remotePublicKey: null,
      data: buf
    })
  }

  send (message, peer) {
    if (this.destroyed) return
    const buf = this.encoding.encode(message)
    this.networker._client.network.sendExtensionNoReply({
      id: 0,
      resourceId: this.resourceId,
      remotePublicKey: peer.remotePublicKey,
      data: buf
    })
  }

  destroy () {
    this.destroyed = true
    this.networker._client.network.unregisterExtensionNoReply({
      id: 0,
      resourceId: this.resourceId
    }, (err) => {
      if (err) this.onerror(err)
      this.networker._extensions.delete(this.resourceId)
    })
  }
}

class RemoteHypercore extends Nanoresource {
  constructor (client, sessions, key, opts) {
    super()
    this.key = key
    this.discoveryKey = null
    this.length = 0
    this.byteLength = 0
    this.writable = false
    this.sparse = true
    this.peers = []
    this.valueEncoding = null
    if (opts.valueEncoding) {
      if (typeof opts.valueEncoding === 'string') this.valueEncoding = codecs(opts.valueEncoding)
      else this.valueEncoding = opts.valueEncoding
    }

    this.weak = !!opts.weak
    this.lazy = !!opts.lazy
    this[PROMISES] = true

    this._client = client
    this._sessions = sessions
    this._name = opts.name
    this._id = this.lazy ? undefined : this._sessions.create(this)
    this._extensions = new Map()
    this._onwaits = new FreeMap(1)

    // Track listeners for some events and enable/disable watching.
    this.on('newListener', (event) => {
      if (event === 'download' && !this.listenerCount(event)) {
        this._watchDownloads()
      }
      if (event === 'upload' && !this.listenerCount(event)) {
        this._watchUploads()
      }
    })
    this.on('removeListener', (event) => {
      if (event === 'download' && !this.listenerCount(event)) {
        this._unwatchDownloads()
      }
      if (event === 'upload' && !this.listenerCount(event)) {
        this._unwatchUploads()
      }
    })

    if (!this.lazy) this.ready(() => {})

    if (this.sparse && opts.eagerUpdate) {
      const self = this
      this.update({ ifAvailable: false }, function loop (err) {
        if (err) self.emit('update-error', err)
        self.update(loop)
      })
    }
  }

  ready (cb) {
    return maybe(cb, this.open())
  }

  [inspect] (depth, opts) {
    var indent = ''
    if (typeof opts.indentationLvl === 'number') {
      while (indent.length < opts.indentationLvl) indent += ' '
    }
    return 'RemoteHypercore(\n' +
      indent + '  key: ' + opts.stylize(this.key && this.key.toString('hex'), 'string') + '\n' +
      indent + '  discoveryKey: ' + opts.stylize(this.discoveryKey && this.discoveryKey.toString('hex'), 'string') + '\n' +
      indent + '  opened: ' + opts.stylize(this.opened, 'boolean') + '\n' +
      indent + '  writable: ' + opts.stylize(this.writable, 'boolean') + '\n' +
      indent + '  length: ' + opts.stylize(this.length, 'number') + '\n' +
      indent + '  byteLength: ' + opts.stylize(this.byteLength, 'number') + '\n' +
      indent + '  peers: ' + opts.stylize(this.peers.length, 'number') + '\n' +
      indent + ')'
  }

  // Nanoresource Methods

  async _open () {
    if (this.lazy) this._id = this._sessions.create(this)
    const rsp = await this._client.corestore.open({
      id: this._id,
      name: this._name,
      key: this.key,
      weak: this.weak
    })
    this.key = rsp.key
    this.discoveryKey = rsp.discoveryKey
    this.writable = rsp.writable
    this.length = rsp.length
    this.byteLength = rsp.byteLength
    if (rsp.peers) this.peers = rsp.peers
    this.emit('ready')
  }

  async _close () {
    await this._client.hypercore.close({ id: this._id })
    this._sessions.delete(this._id)
    this.emit('close')
  }

  // Events

  _onwait (id, seq) {
    const onwait = this._onwaits.get(id)
    if (onwait) {
      this._onwaits.free(id)
      onwait(seq)
    }
  }

  _onappend (rsp) {
    this.length = rsp.length
    this.byteLength = rsp.byteLength
    this.emit('append')
  }

  _onpeeropen (peer) {
    const remotePeer = new RemoteHypercorePeer(peer.type, peer.remoteAddress, peer.remotePublicKey)
    this.peers.push(remotePeer)
    this.emit('peer-add', remotePeer) // compat
    this.emit('peer-open', remotePeer)
  }

  _onpeerremove (peer) {
    const idx = this._indexOfPeer(peer.remotePublicKey)
    if (idx === -1) throw new Error('A peer was removed that was not previously added.')
    const remotePeer = this.peers[idx]
    this.peers.splice(idx, 1)
    this.emit('peer-remove', remotePeer)
  }

  _onextension ({ resourceId, remotePublicKey, data }) {
    const idx = this._indexOfPeer(remotePublicKey)
    if (idx === -1) return
    const remotePeer = this.peers[idx]
    const ext = this._extensions.get(resourceId)
    if (ext.destroyed) return
    ext.onmessage(data, remotePeer)
  }

  _ondownload (rsp) {
    // TODO: Add to local bitfield?
    this.emit('download', rsp.seq, {length: rsp.byteLength, byteLength: rsp.byteLength})
  }

  _onupload (rsp) {
    // TODO: Add to local bitfield?
    this.emit('upload', rsp.seq, {length: rsp.byteLength, byteLength: rsp.byteLength})
  }

  // Private Methods

  _indexOfPeer (remotePublicKey) {
    for (let i = 0; i < this.peers.length; i++) {
      if (remotePublicKey.equals(this.peers[i].remotePublicKey)) return i
    }
    return -1
  }

  async _append (blocks) {
    if (!this.opened) await this.open()
    if (this.closed) throw new Error('Feed is closed')

    if (!Array.isArray(blocks)) blocks = [blocks]
    if (this.valueEncoding) blocks = blocks.map(b => this.valueEncoding.encode(b))
    const rsp = await this._client.hypercore.append({
      id: this._id,
      blocks
    })
    return rsp.seq
  }

  async _get (seq, opts, resourceId) {
    if (!this.opened) await this.open()
    if (this.closed) throw new Error('Feed is closed')

    let onWaitId = 0
    const onwait = opts && opts.onwait
    if (onwait) onWaitId = this._onwaits.add(onwait)

    let rsp

    try {
      rsp = await this._client.hypercore.get({
        ...opts,
        seq,
        id: this._id,
        resourceId,
        onWaitId
      })
    } finally {
      if (onWaitId !== 0 && onwait === this._onwaits.get(onWaitId)) this._onwaits.free(onWaitId)
    }

    if (opts && opts.valueEncoding) return codecs(opts.valueEncoding).decode(rsp.block)
    if (this.valueEncoding) return this.valueEncoding.decode(rsp.block)
    return rsp.block
  }

  async _cancel (resourceId) {
    try {
      if (!this.opened) await this.open()
      if (this.closed) return
    } catch (_) {}

    this._client.hypercore.cancelNoReply({
      id: this._id,
      resourceId
    })
  }

  async _update (opts) {
    if (!this.opened) await this.open()
    if (this.closed) throw new Error('Feed is closed')

    if (typeof opts === 'number') opts = { minLength: opts }
    if (!opts) opts = {}
    if (typeof opts.minLength !== 'number') opts.minLength = this.length + 1
    return await this._client.hypercore.update({
      ...opts,
      id: this._id
    })
  }

  async _seek (byteOffset, opts) {
    if (!this.opened) await this.open()
    if (this.closed) throw new Error('Feed is closed')

    const rsp = await this._client.hypercore.seek({
      byteOffset,
      ...opts,
      id: this._id
    })
    return {
      seq: rsp.seq,
      blockOffset: rsp.blockOffset
    }
  }

  async _has (seq) {
    if (!this.opened) await this.open()
    if (this.closed) throw new Error('Feed is closed')

    const rsp = await this._client.hypercore.has({
      seq,
      id: this._id
    })
    return rsp.has
  }

  async _download (range, resourceId) {
    if (!this.opened) await this.open()
    if (this.closed) throw new Error('Feed is closed')

    return this._client.hypercore.download({ ...range, id: this._id, resourceId })
  }

  async _undownload (resourceId) {
    try {
      if (!this.opened) await this.open()
      if (this.closed) return
    } catch (_) {}

    return this._client.hypercore.undownloadNoReply({ id: this._id, resourceId })
  }

  async _downloaded (start, end) {
    if (!this.opened) await this.open()
    if (this.closed) throw new Error('Feed is closed')
    const rsp = await this._client.hypercore.downloaded({ id: this._id, start, end })
    return rsp.bytes
  }

  async _watchDownloads () {
    try {
      if (!this.opened) await this.open()
      if (this.closed) return
      this._client.hypercore.watchDownloadsNoReply({ id: this._id })
    } catch (_) {}
  }

  async _unwatchDownloads () {
    try {
      if (!this.opened) await this.open()
      if (this.closed) return
      this._client.hypercore.unwatchDownloadsNoReply({ id: this._id })
    } catch (_) {}
  }

  async _watchUploads () {
    try {
      if (!this.opened) await this.open()
      if (this.closed) return
      this._client.hypercore.watchUploadsNoReply({ id: this._id })
    } catch (_) {}
  }

  async _unwatchUploads () {
    try {
      if (!this.opened) await this.open()
      if (this.closed) return
      this._client.hypercore.unwatchUploadsNoReply({ id: this._id })
    } catch (_) {}
  }

  // Public Methods

  append (blocks, cb) {
    return maybeOptional(cb, this._append(blocks))
  }

  get (seq, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = null
    }
    if (!(seq >= 0)) throw new Error('seq must be a positive number')

    const resourceId = this._sessions.createResourceId()
    const prom = this._get(seq, opts, resourceId)
    prom.resourceId = resourceId
    maybe(cb, prom)
    return prom
  }

  update (opts, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = null
    }
    return maybeOptional(cb, this._update(opts))
  }

  seek (byteOffset, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = null
    }
    const seekProm = this._seek(byteOffset, opts)
    if (!cb) return seekProm
    seekProm.then(
      ({ seq, blockOffset }) => process.nextTick(cb, null, seq, blockOffset),
      err => process.nextTick(cb, err)
    )
  }

  has (seq, cb) {
    return maybe(cb, this._has(seq))
  }

  cancel (get) {
    if (typeof get.resourceId !== 'number') throw new Error('Must pass a get return value')
    return this._cancel(get.resourceId)
  }

  createReadStream (opts) {
    return new ReadStream(this, opts)
  }

  createWriteStream (opts) {
    return new WriteStream(this, opts)
  }

  download (range, cb) {
    if (typeof range === 'number') range = { start: range, end: range + 1 }
    if (!range) range = {}
    if (Array.isArray(range)) range = { blocks: range }

    // much easier to run this in the client due to pbuf defaults
    if (range.blocks && typeof range.start !== 'number') {
      let min = -1
      let max = 0

      for (let i = 0; i < range.blocks.length; i++) {
        const blk = range.blocks[i]
        if (min === -1 || blk < min) min = blk
        if (blk >= max) max = blk + 1
      }

      range.start = min === -1 ? 0 : min
      range.end = max
    }

    // massage end = -1, over to something more protobuf friendly
    if (range.end === undefined || range.end === -1) {
      range.end = 0
      range.live = true
    }

    const resourceId = this._sessions.createResourceId()

    const prom = this._download(range, resourceId)
    prom.catch(noop) // optional promise due to the hypercore signature
    prom.resourceId = resourceId

    maybe(cb, prom)
    return prom // always return prom as that one is the "cancel" token
  }

  undownload (download) {
    if (typeof download.resourceId !== 'number') throw new Error('Must pass a download return value')
    this._undownload(download.resourceId)
  }

  downloaded (start, end, cb) {
    if (typeof start === 'function') {
      start = null
      end = null
      cb = start
    } else if (typeof end === 'function') {
      end = null
      cb = end
    }
    return maybe(cb, this._downloaded(start, end))
  }

  lock (onlocked) {
    // TODO: refactor so this can be opened without waiting for open
    if (!this.opened) throw new Error('Cannot acquire a lock for an unopened feed')

    const prom = this._client.hypercore.acquireLock({ id: this._id })

    if (onlocked) {
      const release = (cb, err, val) => { // mutexify interface
        this._client.hypercore.releaseLockNoReply({ id: this._id })
        if (cb) cb(err, val)
      }

      prom.then(() => process.nextTick(onlocked, release), noop)
      return
    }

    return prom.then(() => () => this._client.hypercore.releaseLockNoReply({ id: this._id }))
  }

  // TODO: Unimplemented methods

  registerExtension (name, opts) {
    const ext = new RemoteHypercoreExtension(this, name, opts)
    this._extensions.set(ext.resourceId, ext)
    return ext
  }

  replicate () {
    throw new Error('Cannot call replicate on a RemoteHyperdrive')
  }
}

class RemoteHypercorePeer {
  constructor (type, remoteAddress, remotePublicKey) {
    this.type = type
    this.remoteAddress = remoteAddress
    this.remotePublicKey = remotePublicKey
  }
}

class RemoteHypercoreExtension {
  constructor (feed, name, opts = {}) {
    if (typeof name === 'object') {
      opts = name
      name = opts.name
    }
    this.feed = feed
    this.resourceId = feed._sessions.createResourceId()
    this.name = name
    this.encoding = codecs((opts && opts.encoding) || 'binary')
    this.destroyed = false

    this.onerror = opts.onerror || noop
    this.onmessage = noop
    if (opts.onmessage) {
      this.onmessage = (message, peer) => {
        try {
          message = this.encoding.decode(message)
        } catch (err) {
          return this.onerror(err)
        }
        return opts.onmessage(message, peer)
      }
    }

    const reg = () => {
      this.feed._client.hypercore.registerExtensionNoReply({
        id: this.feed._id,
        resourceId: this.resourceId,
        name: this.name
      })
    }

    if (this.feed._id !== undefined) {
      reg()
    } else {
      this.feed.ready((err) => {
        if (err) return this.onerror(err)
        reg()
      })
    }
  }

  broadcast (message) {
    const buf = this.encoding.encode(message)
    if (this.feed._id === undefined || this.destroyed) return
    this.feed._client.hypercore.sendExtensionNoReply({
      id: this.feed._id,
      resourceId: this.resourceId,
      remotePublicKey: null,
      data: buf
    })
  }

  send (message, peer) {
    if (this.feed._id === undefined || this.destroyed) return
    const buf = this.encoding.encode(message)
    this.feed._client.hypercore.sendExtensionNoReply({
      id: this.feed._id,
      resourceId: this.resourceId,
      remotePublicKey: peer.remotePublicKey,
      data: buf
    })
  }

  destroy () {
    this.destroyed = true
    this.feed.ready((err) => {
      if (err) return this.onerror(err)
      this.feed._client.hypercore.unregisterExtensionNoReply({
        id: this.feed._id,
        resourceId: this.resourceId
      }, err => {
        if (err) this.onerror(err)
        this.feed._extensions.delete(this.resourceId)
      })
    })
  }
}

module.exports = class HyperspaceClient {
  constructor (opts = {}) {
    const sessions = new Sessions()

    this._socketOpts = getNetworkOptions(opts)
    this._client = HRPC.connect(this._socketOpts)
    this._corestore = new RemoteCorestore({ client: this._client, sessions })

    this.network = new RemoteNetworker({ client: this._client, sessions })
    this.corestore = (name) => this._corestore.namespace(name)
    // Exposed like this so that you can destructure: const { replicate } = new Client()
    this.replicate = (core, cb) => maybeOptional(cb, this._replicate(core))
  }

  static async serverReady (opts) {
    const sock = getNetworkOptions(opts)
    return new Promise((resolve) => {
      retry()

      function retry () {
        const socket = net.connect(sock)
        let connected = false

        socket.on('connect', function () {
          connected = true
          socket.destroy()
        })
        socket.on('error', socket.destroy)
        socket.on('close', function () {
          if (connected) return resolve()
          setTimeout(retry, 100)
        })
      }
    })
  }

  status (cb) {
    return maybe(cb, this._client.hyperspace.status())
  }

  stop (cb) {
    return maybe(cb, this._client.hyperspace.stopNoReply())
  }

  close () {
    return this._client.destroy()
  }

  ready (cb) {
    return maybe(cb, this.network.ready())
  }

  async _replicate (core) {
    await this.network.configure(core, {
      announce: true,
      lookup: true
    })
    try {
      await core.update({ ifAvailable: true })
    } catch (_) {
      // If this update fails, the error can be ignored.
    }
  }
}

function noop () {}

function randomNamespace () { // does *not* have to be secure
  let ns = ''
  while (ns < 64) ns += Math.random().toString(16).slice(2)
  return ns.slice(0, 64)
}

function maybeOptional (cb, prom) {
  prom = maybe(cb, prom)
  if (prom) prom.catch(noop)
  return prom
}
