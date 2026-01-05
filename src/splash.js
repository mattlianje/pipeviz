export function renderSplashGraph() {
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark'
    const textColor = isDark ? '#e0e0e0' : '#000000'
    const dot = `digraph {
        rankdir=LR
        bgcolor="transparent"
        node [fontname="Arial" fontsize="10" fontcolor="${textColor}"]
        edge [color="#999999"]
        "raw_events" [shape=ellipse style=filled fillcolor="#f3e5f5" color="#7b1fa2"]
        "etl-job" [shape=box style="filled,rounded" fillcolor="#e3f2fd" color="#1976d2" fontname="Arial"]
        "cleaned_events" [shape=ellipse style=filled fillcolor="#f3e5f5" color="#7b1fa2"]
        "raw_events" -> "etl-job"
        "etl-job" -> "cleaned_events"
    }`
    d3.select("#splash-graph").graphviz()
        .width(260)
        .height(110)
        .fit(true)
        .renderDot(dot)
}
