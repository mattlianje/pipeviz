// d3, d3-graphviz and the graphviz wasm are bundled into the single-file build
// instead of loaded from a CDN. The rest of the app uses the d3 global.
import * as d3 from 'd3'
import 'd3-graphviz'

window.d3 = d3
