import { generateGraphvizDot } from './graph.js'

export function updateDotView() {
    const dotOutput = document.getElementById('dot-output')
    if (dotOutput) {
        dotOutput.value = generateGraphvizDot()
    }
}

export function copyDotToClipboard(event) {
    const dotOutput = document.getElementById('dot-output')
    if (dotOutput) {
        navigator.clipboard.writeText(dotOutput.value).then(() => {
            const btn = event?.target || document.activeElement
            const originalText = btn.textContent
            btn.textContent = 'Copied!'
            setTimeout(() => {
                btn.textContent = originalText
            }, 2000)
        }).catch(() => {
            // Fallback for older browsers
            dotOutput.select()
            dotOutput.setSelectionRange(0, 99999)
        })
    }
}
