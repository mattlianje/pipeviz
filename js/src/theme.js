import { updateGraph } from './graph.js'

export function toggleTheme() {
    const html = document.documentElement
    const btn = document.getElementById('theme-toggle')
    if (html.getAttribute('data-theme') === 'dark') {
        html.removeAttribute('data-theme')
        btn.textContent = '\u263C'
        localStorage.setItem('pipeviz-theme', 'light')
    } else {
        html.setAttribute('data-theme', 'dark')
        btn.textContent = '\u263D'
        localStorage.setItem('pipeviz-theme', 'dark')
    }
    updateGraph(true)
}

export function initTheme() {
    const saved = localStorage.getItem('pipeviz-theme')
    if (saved === 'dark') {
        document.documentElement.setAttribute('data-theme', 'dark')
        document.getElementById('theme-toggle').textContent = '\u263D'
    }
}
