import { escapeHtml } from './escape.js'

// Floating two-sided slider under the graph: how many hops of lineage to light
// upstream (left) and downstream (right) of the selected node. Built once per
// node and updated in place, so a drag isn't interrupted by re-renders.

const STEP = 22 // px between stops
const MAX_STOPS = 8 // past this, the last stop is "all"

let current = null // { name, maxUp, maxDown, onReach, onFocus }

// Stops are positions 0..n out from the node; 0 = none, n = all.
function stopCount(max) {
    return max > MAX_STOPS ? MAX_STOPS + 1 : max
}
const valueToPos = (value, max) => Math.min(value, stopCount(max))
const posToValue = (pos, max) => (pos >= stopCount(max) ? Infinity : pos)

function stopLabel(pos, max) {
    if (max > MAX_STOPS && pos === stopCount(max)) return 'all'
    return String(pos)
}

function valueText(value, max) {
    const pos = valueToPos(value, max)
    if (pos === 0) return 'none'
    if (pos >= stopCount(max)) return 'all'
    return `${pos} hop${pos > 1 ? 's' : ''}`
}

function trackHtml(dir, max) {
    const n = stopCount(max)
    if (n === 0) return `<div class="lc-track lc-track-empty" data-dir="${dir}">none</div>`
    let ticks = ''
    for (let p = 1; p <= n; p++) {
        ticks += `<span class="lc-tick" style="--p: ${p}"><span class="lc-tick-label">${stopLabel(p, max)}</span></span>`
    }
    const label = dir === 'up' ? 'Upstream hops' : 'Downstream hops'
    return `<div class="lc-track" data-dir="${dir}" style="--n: ${n}" role="slider" tabindex="0" aria-label="${label}" aria-valuemin="0" aria-valuemax="${n}">
        <div class="lc-rail"></div>
        <div class="lc-fill"></div>
        ${ticks}
        <div class="lc-handle"></div>
    </div>`
}

function build(root, info) {
    root.innerHTML = `
        <span class="lc-label lc-label-up">Upstream <span class="lc-count" data-dir="up"></span></span>
        ${trackHtml('up', info.maxUp)}
        <span class="lc-node" title="${escapeHtml(info.name)}"></span>
        ${trackHtml('down', info.maxDown)}
        <span class="lc-label lc-label-down">Downstream <span class="lc-count" data-dir="down"></span></span>
        <span class="lc-others" role="group" aria-label="Nodes outside this lineage" title="Nodes outside this lineage (H)">
            <span class="lc-others-label">Others</span>
            <button class="lc-others-btn" data-hide="false">Dim</button>
            <button class="lc-others-btn" data-hide="true">Hide</button>
        </span>
        <button class="lc-focus" title="Re-lay out just this lineage (F, or double-click the node)"></button>`

    root.querySelectorAll('.lc-track[role="slider"]').forEach((track) => {
        const dir = track.dataset.dir
        const max = dir === 'up' ? info.maxUp : info.maxDown
        const n = stopCount(max)
        const set = (pos) => {
            pos = Math.max(0, Math.min(n, pos))
            const value = posToValue(pos, max)
            if (value !== current.values[dir]) current.onReach(dir, value)
        }
        // Distance out from the node, in stops.
        const posAt = (clientX) => {
            const r = track.getBoundingClientRect()
            return Math.round((dir === 'up' ? r.right - clientX : clientX - r.left) / STEP)
        }

        track.addEventListener('pointerdown', (e) => {
            track.setPointerCapture(e.pointerId)
            track.classList.add('dragging')
            set(posAt(e.clientX))
        })
        track.addEventListener('pointermove', (e) => {
            if (track.hasPointerCapture(e.pointerId)) set(posAt(e.clientX))
        })
        const end = (e) => {
            if (track.hasPointerCapture(e.pointerId)) track.releasePointerCapture(e.pointerId)
            track.classList.remove('dragging')
        }
        track.addEventListener('pointerup', end)
        track.addEventListener('pointercancel', end)

        // Arrow keys follow the layout: left goes further upstream.
        track.addEventListener('keydown', (e) => {
            const pos = valueToPos(current.values[dir], max)
            const outward = dir === 'up' ? 'ArrowLeft' : 'ArrowRight'
            const inward = dir === 'up' ? 'ArrowRight' : 'ArrowLeft'
            if (e.key === outward || e.key === 'ArrowUp') set(pos + 1)
            else if (e.key === inward || e.key === 'ArrowDown') set(pos - 1)
            else if (e.key === 'Home') set(0)
            else if (e.key === 'End') set(n)
            else return
            e.preventDefault()
        })
    })

    root.querySelector('.lc-focus').addEventListener('click', () => current.onFocus())
    root.querySelectorAll('.lc-others-btn').forEach((btn) => {
        btn.addEventListener('click', () => current.onHide(btn.dataset.hide === 'true'))
    })
}

// `info` null hides the control.
export function updateLineageControl(info) {
    const root = document.getElementById('lineage-ctl')
    if (!root) return
    if (!info) {
        root.hidden = true
        current = null
        return
    }

    const rebuild =
        !current || current.name !== info.name || current.maxUp !== info.maxUp || current.maxDown !== info.maxDown
    current = { ...info, values: { up: info.reachUp, down: info.reachDown } }
    if (rebuild) build(root, info)
    root.hidden = false

    for (const dir of ['up', 'down']) {
        const max = dir === 'up' ? info.maxUp : info.maxDown
        const value = current.values[dir]
        const count = dir === 'up' ? info.upCount : info.downCount
        root.querySelector(`.lc-count[data-dir="${dir}"]`).textContent = count
        root.querySelector(`.lc-label-${dir}`).classList.toggle('lc-label-off', count === 0)

        const track = root.querySelector(`.lc-track[data-dir="${dir}"][role="slider"]`)
        if (!track) continue
        const pos = valueToPos(value, max)
        track.style.setProperty('--pos', pos)
        track.setAttribute('aria-valuenow', pos)
        track.setAttribute('aria-valuetext', valueText(value, max))
        track.querySelectorAll('.lc-tick').forEach((t, i) => t.classList.toggle('on', i + 1 <= pos))
    }

    root.querySelectorAll('.lc-others-btn').forEach((btn) => {
        const on = (btn.dataset.hide === 'true') === info.hiding
        btn.classList.toggle('active', on)
        btn.setAttribute('aria-pressed', on)
    })

    const focus = root.querySelector('.lc-focus')
    focus.textContent = info.focused ? 'Unfocus' : 'Focus'
    focus.classList.toggle('active', info.focused)
}
