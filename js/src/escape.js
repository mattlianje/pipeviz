// Configs can come from any URL (?src, ?url, ?gh, ?c), so every config-derived
// value that reaches innerHTML, an inline handler, or DOT source goes through
// one of these.

const HTML_ESCAPES = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }

export function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (c) => HTML_ESCAPES[c])
}

// Argument to an inline handler: onclick="selectNode(${jsArg(name)})"
export function jsArg(value) {
    return escapeHtml(JSON.stringify(String(value ?? '')))
}

// http(s) only. javascript:, data: and anything unparseable become '#'.
export function safeUrl(value) {
    try {
        const url = new URL(String(value), window.location.href)
        return url.protocol === 'http:' || url.protocol === 'https:' ? url.href : '#'
    } catch {
        return '#'
    }
}

// Contents of a double-quoted DOT string. A trailing backslash would escape
// the closing quote, so backslashes are swapped for a lookalike.
export function escapeDot(value) {
    return String(value ?? '')
        .replace(/\\/g, '⧵')
        .replace(/"/g, '\\"')
}
