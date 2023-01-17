"use strict";(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[9671],{3905:(e,r,t)=>{t.d(r,{Zo:()=>d,kt:()=>m});var a=t(7294);function n(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function o(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);r&&(a=a.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,a)}return t}function i(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?o(Object(t),!0).forEach((function(r){n(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):o(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function s(e,r){if(null==e)return{};var t,a,n=function(e,r){if(null==e)return{};var t,a,n={},o=Object.keys(e);for(a=0;a<o.length;a++)t=o[a],r.indexOf(t)>=0||(n[t]=e[t]);return n}(e,r);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)t=o[a],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(n[t]=e[t])}return n}var l=a.createContext({}),c=function(e){var r=a.useContext(l),t=r;return e&&(t="function"==typeof e?e(r):i(i({},r),e)),t},d=function(e){var r=c(e.components);return a.createElement(l.Provider,{value:r},e.children)},p="mdxType",g={inlineCode:"code",wrapper:function(e){var r=e.children;return a.createElement(a.Fragment,{},r)}},u=a.forwardRef((function(e,r){var t=e.components,n=e.mdxType,o=e.originalType,l=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),p=c(t),u=n,m=p["".concat(l,".").concat(u)]||p[u]||g[u]||o;return t?a.createElement(m,i(i({ref:r},d),{},{components:t})):a.createElement(m,i({ref:r},d))}));function m(e,r){var t=arguments,n=r&&r.mdxType;if("string"==typeof e||n){var o=t.length,i=new Array(o);i[0]=u;var s={};for(var l in r)hasOwnProperty.call(r,l)&&(s[l]=r[l]);s.originalType=e,s[p]="string"==typeof e?e:n,i[1]=s;for(var c=2;c<o;c++)i[c]=t[c];return a.createElement.apply(null,i)}return a.createElement.apply(null,t)}u.displayName="MDXCreateElement"},9881:(e,r,t)=>{t.r(r),t.d(r,{contentTitle:()=>i,default:()=>d,frontMatter:()=>o,metadata:()=>s,toc:()=>l});var a=t(7462),n=(t(7294),t(3905));const o={sidebar_position:1},i="Introduction",s={unversionedId:"intro",id:"intro",isDocsHomePage:!1,title:"Introduction",description:"Dagger or Data Aggregator is an easy-to-use, configuration over code, cloud-native framework built on top of Apache Flink",source:"@site/docs/intro.md",sourceDirName:".",slug:"/intro",permalink:"/dagger/docs/intro",editUrl:"https://github.com/odpf/dagger/edit/master/docs/docs/intro.md",tags:[],version:"current",sidebarPosition:1,frontMatter:{sidebar_position:1},sidebar:"docsSidebar",next:{title:"Roadmap",permalink:"/dagger/docs/roadmap"}},l=[{value:"Key Features",id:"key-features",children:[]},{value:"Usecases",id:"usecases",children:[]},{value:"Where to go from here",id:"where-to-go-from-here",children:[]}],c={toc:l};function d(e){let{components:r,...o}=e;return(0,n.kt)("wrapper",(0,a.Z)({},c,o,{components:r,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"introduction"},"Introduction"),(0,n.kt)("p",null,"Dagger or Data Aggregator is an easy-to-use, configuration over code, cloud-native framework built on top of Apache Flink\nfor stateful processing of data. With Dagger, you don't need to write custom applications or complicated code to process\ndata as a stream. Instead, you can write SQL queries and UDFs to do the processing and analysis on streaming data."),(0,n.kt)("p",null,(0,n.kt)("img",{src:t(8201).Z})),(0,n.kt)("h2",{id:"key-features"},"Key Features"),(0,n.kt)("p",null,"Discover why to use Dagger"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("strong",{parentName:"li"},"Processing:")," Dagger can transform, aggregate, join and enrich streaming data, both real-time and historical."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("strong",{parentName:"li"},"Scale:")," Dagger scales in an instant, both vertically and horizontally for high performance streaming sink and zero data drops."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("strong",{parentName:"li"},"Extensibility:")," Add your own sink to dagger with a clearly defined interface or choose from already provided ones. Use Kafka and/or Parquet Files as stream sources."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("strong",{parentName:"li"},"Flexibility:")," Add custom business logic in form of plugins ","(","UDFs, Transformers, Preprocessors and Post Processors",")"," independent of the core logic."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("strong",{parentName:"li"},"Metrics:")," Always know what\u2019s going on with your deployment with built-in ",(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/reference/metrics"},"monitoring")," of throughput, response times, errors and more.")),(0,n.kt)("h2",{id:"usecases"},"Usecases"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html"},"Map reduce with SQL")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sql.html"},"Aggregation with SQL"),", ",(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/guides/use_udf"},"UDFs")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/advance/post_processor"},"Enrichment with Post Processors")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/reference/transformers#HashTransformer"},"Data Masking with Hash Transformer")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/reference/transformers#DeDuplicationTransformer"},"Data Deduplication with Transformer")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/advance/longbow"},"Realtime long window processing with Longbow"))),(0,n.kt)("p",null,"To know more, follow the detailed ",(0,n.kt)("a",{parentName:"p",href:"https://odpf.gitbook.io/dagger"},"documentation"),"."),(0,n.kt)("h2",{id:"where-to-go-from-here"},"Where to go from here"),(0,n.kt)("p",null,"Explore the following resources to get started with Dagger:"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/guides/overview"},"Guides")," provides guidance on ",(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/guides/overview"},"creating Dagger")," with different sinks."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/concepts/overview"},"Concepts")," describes all important Dagger concepts."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/advance/overview"},"Advance")," contains details regarding advance features of Dagger."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/reference/overview"},"Reference")," contains details about configurations, metrics and other aspects of Dagger."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/contribute/contribution"},"Contribute")," contains resources for anyone who wants to contribute to Dagger."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/usecase/overview"},"Usecase")," describes examples use cases which can be solved via Dagger."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"/dagger/docs/examples/overview"},"Examples")," contains tutorials to try out some of Dagger's features with real-world usecases")))}d.isMDXComponent=!0},8201:(e,r,t)=>{t.d(r,{Z:()=>a});const a=t.p+"assets/images/dagger_overview-134afc3329ed14a11011d5a2b06632cb.png"}}]);