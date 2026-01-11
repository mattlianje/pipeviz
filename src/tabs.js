export function setupTabs() {
    document.querySelectorAll('#dashboardTabs button').forEach((tab) => {
        tab.addEventListener('shown.bs.tab', function (e) {
            const tabs = document.getElementById('dashboardTabs')
            const tabName = e.target.id.replace('-tab', '')
            if (e.target.id === 'home-tab') {
                tabs.classList.add('hidden-on-home')
                history.replaceState(null, '', window.location.pathname + window.location.search)
            } else {
                tabs.classList.remove('hidden-on-home')
                history.replaceState(null, '', '#' + tabName)
            }
        })
    })
}

export function activateTabFromHash() {
    const hash = window.location.hash.slice(1)
    if (hash) {
        const tab = document.getElementById(hash + '-tab')
        if (tab) {
            tab.click()
        }
    }
}
