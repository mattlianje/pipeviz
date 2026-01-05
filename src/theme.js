import { renderSplashGraph } from './splash.js'
import { updateGraph } from './graph.js'

export function toggleTheme() {
    const html = document.documentElement
    const btn = document.getElementById('theme-toggle')
    if (html.getAttribute('data-theme') === 'dark') {
        html.removeAttribute('data-theme')
        btn.textContent = '\u2600\uFE0F'
        localStorage.setItem('pipeviz-theme', 'light')
    } else {
        html.setAttribute('data-theme', 'dark')
        btn.textContent = '\uD83C\uDF19'
        localStorage.setItem('pipeviz-theme', 'dark')
    }
    renderSplashGraph()
    updateGraph()
}

export function initTheme() {
    const saved = localStorage.getItem('pipeviz-theme')
    if (saved === 'dark') {
        document.documentElement.setAttribute('data-theme', 'dark')
        document.getElementById('theme-toggle').textContent = '\uD83C\uDF19'
    }
}
