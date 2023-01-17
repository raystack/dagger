"use strict";(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[9361],{3905:(e,t,r)=>{r.d(t,{Zo:()=>p,kt:()=>g});var n=r(7294);function o(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function a(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?a(Object(r),!0).forEach((function(t){o(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):a(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,n,o=function(e,t){if(null==e)return{};var r,n,o={},a=Object.keys(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||(o[r]=e[r]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(o[r]=e[r])}return o}var s=n.createContext({}),c=function(e){var t=n.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},p=function(e){var t=c(e.components);return n.createElement(s.Provider,{value:t},e.children)},m="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},d=n.forwardRef((function(e,t){var r=e.components,o=e.mdxType,a=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),m=c(r),d=o,g=m["".concat(s,".").concat(d)]||m[d]||u[d]||a;return r?n.createElement(g,i(i({ref:t},p),{},{components:r})):n.createElement(g,i({ref:t},p))}));function g(e,t){var r=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=r.length,i=new Array(a);i[0]=d;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[m]="string"==typeof e?e:o,i[1]=l;for(var c=2;c<a;c++)i[c]=r[c];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}d.displayName="MDXCreateElement"},8329:(e,t,r)=>{r.r(t),r.d(t,{contentTitle:()=>i,default:()=>p,frontMatter:()=>a,metadata:()=>l,toc:()=>s});var n=r(7462),o=(r(7294),r(3905));const a={},i="Joining two Kafka topics using Inner join",l={unversionedId:"examples/kafka_inner_join",id:"examples/kafka_inner_join",isDocsHomePage:!1,title:"Joining two Kafka topics using Inner join",description:"About this example",source:"@site/docs/examples/kafka_inner_join.md",sourceDirName:"examples",slug:"/examples/kafka_inner_join",permalink:"/dagger/docs/examples/kafka_inner_join",editUrl:"https://github.com/odpf/dagger/edit/master/docs/docs/examples/kafka_inner_join.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Stream enrichment using ElasticSearch source",permalink:"/dagger/docs/examples/elasticsearch_enrichment"},next:{title:"Contribution Process",permalink:"/dagger/docs/contribute/contribution"}},s=[{value:"About this example",id:"about-this-example",children:[]},{value:"Before Trying This Example",id:"before-trying-this-example",children:[]},{value:"Steps",id:"steps",children:[]}],c={toc:s};function p(e){let{components:t,...r}=e;return(0,o.kt)("wrapper",(0,n.Z)({},c,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"joining-two-kafka-topics-using-inner-join"},"Joining two Kafka topics using Inner join"),(0,o.kt)("h2",{id:"about-this-example"},"About this example"),(0,o.kt)("p",null,"In this example, we will use the Inner joins in Dagger to join the data streams from two different Kafka topics and count the number of booking logs in every 30 second interval from both the sources combined for each service type. By the end of this example we will understand how to use inner joins to combine 2 or more Kafka streams."),(0,o.kt)("h2",{id:"before-trying-this-example"},"Before Trying This Example"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},(0,o.kt)("strong",{parentName:"p"},"We must have Docker installed"),". We can follow ",(0,o.kt)("a",{parentName:"p",href:"https://docs.docker.com/get-docker/"},"this guide")," on how to install and set up Docker in your local machine.")),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("p",{parentName:"li"},"Clone Dagger repository into your local"),(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"git clone https://github.com/odpf/dagger.git\n")))),(0,o.kt)("h2",{id:"steps"},"Steps"),(0,o.kt)("p",null,"Following are the steps for setting up dagger in docker compose -"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"cd into the aggregation directory:",(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"cd dagger/quickstart/examples/aggregation/tumble_window \n"))),(0,o.kt)("li",{parentName:"ol"},"fire this command to spin up the docker compose:",(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"docker compose up \n")),"Hang on for a while as it installs all the required dependencies and starts all the required services. After a while we should see the output of the Dagger SQL query in the terminal, which will be the number of booking logs in every 30 second interval from both the Kafka sources combined, for each service type."),(0,o.kt)("li",{parentName:"ol"},"fire this command to gracefully close the docker compose:",(0,o.kt)("pre",{parentName:"li"},(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"docker compose down \n")),"This will stop and remove all the containers.")),(0,o.kt)("p",null,"Congratulations, we are now able to use Dagger to combine 2 or more Kafka streams.!"))}p.isMDXComponent=!0}}]);