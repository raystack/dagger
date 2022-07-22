/*! For license information please see c4f5d8e4.d1183dff.js.LICENSE.txt */
(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[4195],{2579:(e,t,n)=>{"use strict";n.d(t,{Z:()=>s});var r=n(4184),a=n.n(r),o=n(7294);const i=e=>{const t=a()(e.className,{darkBackground:"dark"===e.background,highlightBackground:"highlight"===e.background,lightBackground:"light"===e.background,paddingAll:e.padding.indexOf("all")>=0,paddingBottom:e.padding.indexOf("bottom")>=0,paddingLeft:e.padding.indexOf("left")>=0,paddingRight:e.padding.indexOf("right")>=0,paddingTop:e.padding.indexOf("top")>=0});let n;return n=e.wrapper?o.createElement("div",{className:"container"},e.children):e.children,o.createElement("div",{className:t,id:e.id},n)};i.defaultProps={background:null,padding:[],wrapper:!0};const s=i},9260:(e,t,n)=>{"use strict";n.d(t,{Z:()=>s});var r=n(4184),a=n.n(r),o=n(7294);class i extends o.Component{renderBlock(e){const t={imageAlign:"left",...e},n=a()("blockElement",this.props.className,{alignCenter:"center"===this.props.align,alignRight:"right"===this.props.align,fourByGridBlock:"fourColumn"===this.props.layout,threeByGridBlock:"threeColumn"===this.props.layout,twoByGridBlock:"twoColumn"===this.props.layout});return o.createElement("div",{className:n,key:t.title},o.createElement("div",{className:"blockContent"},this.renderBlockTitle(t.title),t.content))}renderBlockTitle(e){return e?o.createElement("h2",null,e):null}render(){return o.createElement("div",{className:"gridBlock"},this.props.contents.map(this.renderBlock,this))}}i.defaultProps={align:"left",contents:[],layout:"twoColumn"};const s=i},2841:(e,t,n)=>{"use strict";n.r(t),n.d(t,{default:()=>m});var r=n(7294),a=n(6698),o=n(6010),i=n(2263),s=n(2579),l=n(9260),c=n(4996);const d=()=>{const{siteConfig:e}=(0,i.Z)();return r.createElement("div",{className:"homeHero"},r.createElement("div",{className:"logo"},r.createElement("img",{src:(0,c.Z)("img/pattern.svg")})),r.createElement("div",{className:"container banner"},r.createElement("div",{className:"row"},r.createElement("div",{className:(0,o.Z)("col col--5")},r.createElement("div",{className:"homeTitle"},"Stream processing made easy"),r.createElement("small",{className:"homeSubTitle"},"Configuration over code, cloud-native framework built on top of Apache Flink for stateful processing of real-time and historical streaming data."),r.createElement("a",{className:"button",href:"docs/intro"},"Documentation")),r.createElement("div",{className:(0,o.Z)("col col--1")}),r.createElement("div",{className:(0,o.Z)("col col--6")},r.createElement("div",{className:"text--right"},r.createElement("img",{src:(0,c.Z)("img/query.svg")}))))))};function m(){const{siteConfig:e}=(0,i.Z)();return r.createElement(a.Z,{title:"Stream processing made easy",description:"Stream processing framework"},r.createElement(d,null),r.createElement("main",null,r.createElement(s.Z,{className:"textSection wrapper",background:"light"},r.createElement("h1",null,"Built for scale"),r.createElement("p",null,"Dagger or Data Aggregator is an easy-to-use, configuration over code, cloud-native framework built on top of Apache Flink for stateful processing of both real time and historical streaming data. With Dagger, you don't need to write custom applications to process data as a stream. Instead, you can write SQLs to do the processing and analysis on streaming data."),r.createElement(l.Z,{layout:"threeColumn",contents:[{title:"Reliable & consistent processing",content:r.createElement("div",null,"Provides built-in support for fault-tolerant execution that is consistent and correct regardless of data size, cluster size, processing pattern or pipeline complexity.")},{title:"Robust recovery mechanism",content:r.createElement("div",null,"Checkpoints, Savepoints & State-backup ensure that even in unforeseen circumstances, clusters & jobs can be brought back within minutes.")},{title:"SQL and more",content:r.createElement("div",null,"Define business logic in a query & kick-start your streaming job; but it is not just that, there is support for user-defined functions & pre-defined transformations.")},{title:"Scale",content:r.createElement("div",null,"Dagger scales in an instant, both vertically and horizontally for high performance streaming sink and zero data drops.")},{title:"Extensibility",content:r.createElement("div",null,"Add your own sink to dagger with a clearly defined interface or choose from already provided ones. Use Kafka Source for processing real time data or opt for Parquet Source to stream historical data from Parquet Files.")},{title:"Flexibility",content:r.createElement("div",null,"Add custom business logic in form of plugins (UDFs, Transformers, Preprocessors and Post Processors) independent of the core logic.")}]})),r.createElement(s.Z,{className:"textSection wrapper",background:"dark"},r.createElement("h1",null,"Key Features"),r.createElement("p",null,"Stream processing platform for transforming, aggregating and enriching data in real-time mode with ease of operation & unbelievable reliability. Dagger can deployd in VMs or cloud-native environment to makes resource provisioning and deployment simple & straight-forward, the only limit to your data processing is your imagination."),r.createElement(l.Z,{contents:[{title:"Aggregations",content:r.createElement("div",null,"Supports Tumble & Slide for time-windows. Longbow feature supports large windows upto 30-day.")},{title:"SQL Support",content:r.createElement("div",null,"Query writing made easy through formatting, suggestions, auto-completes and template queries.")},{title:"Stream Enrichment",content:r.createElement("div",null,"Enrich streamed messages from HTTP endpoints or database sources to bring offline & reference data context to real-time processing.")},{title:"Observability",content:r.createElement("div",null,"Always know what\u2019s going on with your deployment with built-in monitoring of throughput, response times, errors and more.")},{title:"Analytics Ecosystem",content:r.createElement("div",null,"Dagger can transform, aggregate, join and enrich data in real-time for operational analytics using InfluxDB, Grafana and others.")},{title:"Stream Transformations",content:r.createElement("div",null,"Convert messages on the fly for a variety of use-cases such as feature engineering.")},{title:"Support for Real Time and Historical Data Streaming",content:r.createElement("div",null,"Use Kafka Source for processing real time data or opt for Parquet Source to stream historical data from Parquet Files.")}]})),r.createElement(s.Z,{className:"textSection wrapper",background:"light"},r.createElement("h1",null,"Proud Users"),r.createElement("p",null,"Dagger was originally created for the Gojek data processing platform, and it has been used, adapted and improved by other teams internally and externally."),r.createElement(l.Z,{className:"logos",layout:"fourColumn",contents:[{content:r.createElement("img",{src:(0,c.Z)("users/gojek.png")})},{content:r.createElement("img",{src:(0,c.Z)("users/midtrans.png")})},{content:r.createElement("img",{src:(0,c.Z)("users/mapan.png")})},{content:r.createElement("img",{src:(0,c.Z)("users/moka.png")})}]}))))}},4184:(e,t)=>{var n;!function(){"use strict";var r={}.hasOwnProperty;function a(){for(var e=[],t=0;t<arguments.length;t++){var n=arguments[t];if(n){var o=typeof n;if("string"===o||"number"===o)e.push(n);else if(Array.isArray(n)){if(n.length){var i=a.apply(null,n);i&&e.push(i)}}else if("object"===o)if(n.toString===Object.prototype.toString)for(var s in n)r.call(n,s)&&n[s]&&e.push(s);else e.push(n.toString())}}return e.join(" ")}e.exports?(a.default=a,e.exports=a):void 0===(n=function(){return a}.apply(t,[]))||(e.exports=n)}()}}]);