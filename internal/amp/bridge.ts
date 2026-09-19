import { appendFileSync, existsSync } from 'node:fs'

// Each launch installs its own entry file; other Amp processes leave it inert.
const launch = 'ACP_GO_AMP_LAUNCH_TOKEN'
export default function (amp) {
  if (process.env.ACP_GO_AMP_INTERNAL_PLUGIN !== launch) return
  const directory = process.env.ACP_GO_AMP_INTERNAL_BRIDGE
  const threadID = process.env.ACP_GO_AMP_INTERNAL_THREAD
  let thread, subscription, disposed = false, cancelling = false
  let epoch = 0, ended = false, reading = false
  const emit = (type, data = {}) => {
    if (!disposed) appendFileSync(`${directory}/events`, JSON.stringify({ type, threadId: threadID, ...data }) + '\n', { mode: 0o600 })
  }
  const fail = () => emit('error')
  const quiet = state => state === 'idle' || state === 'error'
  async function snapshot(type) {
    if (reading || !thread || disposed) return
    reading = true
    try {
      const state = await thread.state.get()
      const generation = epoch
      if (!quiet(state)) {
        if (type === 'ready') emit(type, { state, messages: [] })
        return
      }
      const messages = []
      for (let offset = 0; ; offset += 20) {
        const page = await thread.messages({ full: true, from: 'start', offset, limit: 20 })
        messages.push(...page)
        if (page.length < 20) break
      }
      if (generation !== epoch || state !== await thread.state.get()) throw new Error('thread changed')
      emit(type, { state, messages })
    } catch { fail() } finally { reading = false }
  }
  async function cancel() {
    if (!thread || cancelling || !existsSync(`${directory}/cancel`)) return
    cancelling = true
    try {
      await thread.cancel()
      emit('cancel.ack')
      if (ended) await snapshot('settled')
    } catch { fail() }
  }
  const timer = setInterval(() => { void cancel() }, 25)
  amp.on('session.start', (event, ctx) => {
    if (event.thread.id !== threadID || thread) return
    thread = ctx.thread
    subscription = thread.state.subscribe(state => {
      epoch++
      if (ended && quiet(state)) void snapshot('settled')
    })
    // Subscribe before the read, outside the hook that initializes the session.
    setTimeout(() => { void snapshot('ready') }, 0)
  })
  amp.on('agent.start', async (event, ctx) => {
    if (event.thread.id !== threadID) return
    epoch++
    ended = false
    emit('start', { id: event.id })
    if (existsSync(`${directory}/cancel`)) {
      await ctx.thread.cancel()
      cancelling = true
      emit('cancel.ack')
    }
  })
  amp.on('agent.end', event => {
    if (event.thread.id !== threadID) return
    epoch++
    ended = true
    emit('end', { id: event.id, status: event.status, messages: event.messages })
    // The native state transition follows this hook; never wait inside it.
    setTimeout(() => { void snapshot('settled') }, 0)
  })
  amp.onDispose(() => {
    disposed = true
    clearInterval(timer)
    subscription?.unsubscribe()
  })
}
