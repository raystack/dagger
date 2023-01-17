"use strict";(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[841],{3905:(e,t,r)=>{r.d(t,{Zo:()=>c,kt:()=>g});var a=r(7294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function l(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},o=Object.keys(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)r=o[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var s=a.createContext({}),p=function(e){var t=a.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},c=function(e){var t=p(e.components);return a.createElement(s.Provider,{value:t},e.children)},d="mdxType",u={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,o=e.originalType,s=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),d=p(r),m=n,g=d["".concat(s,".").concat(m)]||d[m]||u[m]||o;return r?a.createElement(g,i(i({ref:t},c),{},{components:r})):a.createElement(g,i({ref:t},c))}));function g(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=r.length,i=new Array(o);i[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[d]="string"==typeof e?e:n,i[1]=l;for(var p=2;p<o;p++)i[p]=r[p];return a.createElement.apply(null,i)}return a.createElement.apply(null,r)}m.displayName="MDXCreateElement"},1064:(e,t,r)=>{r.r(t),r.d(t,{contentTitle:()=>i,default:()=>c,frontMatter:()=>o,metadata:()=>l,toc:()=>s});var a=r(7462),n=(r(7294),r(3905));const o={},i="Removing duplicate records using Transformers",l={unversionedId:"examples/deduplication_transformer",id:"examples/deduplication_transformer",isDocsHomePage:!1,title:"Removing duplicate records using Transformers",description:"About this example",source:"@site/docs/examples/deduplication_transformer.md",sourceDirName:"examples",slug:"/examples/deduplication_transformer",permalink:"/dagger/docs/examples/deduplication_transformer",editUrl:"https://github.com/odpf/dagger/edit/master/docs/docs/examples/deduplication_transformer.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Data Aggregation using a Tumble Window",permalink:"/dagger/docs/examples/aggregation_tumble_window"},next:{title:"Distance computation using Java UDF",permalink:"/dagger/docs/examples/distance_java_udf"}},s=[{value:"About this example",id:"about-this-example",children:[]},{value:"Before Trying This Example",id:"before-trying-this-example",children:[]},{value:"Steps",id:"steps",children:[]}],p={toc:s};function c(e){let{components:t,...r}=e;return(0,n.kt)("wrapper",(0,a.Z)({},p,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"removing-duplicate-records-using-transformers"},"Removing duplicate records using Transformers"),(0,n.kt)("h2",{id:"about-this-example"},"About this example"),(0,n.kt)("p",null,"In this example, we will use the DeDuplication Transformer in Dagger to remove the booking orders (as Kafka records) having duplicate ",(0,n.kt)("inlineCode",{parentName:"p"},"order_number"),". By the end of this example we will understand how to use Dagger to remove duplicate data from Kafka source."),(0,n.kt)("h2",{id:"before-trying-this-example"},"Before Trying This Example"),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("strong",{parentName:"p"},"We must have Docker installed"),". We can follow ",(0,n.kt)("a",{parentName:"p",href:"https://docs.docker.com/get-docker/"},"this guide")," on how to install and set up Docker in your local machine.")),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Clone Dagger repository into your local"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"git clone https://github.com/odpf/dagger.git\n")))),(0,n.kt)("h2",{id:"steps"},"Steps"),(0,n.kt)("p",null,"Following are the steps for setting up dagger in docker compose -"),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},"cd into the aggregation directory:",(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"cd dagger/quickstart/examples/aggregation/tumble_window \n"))),(0,n.kt)("li",{parentName:"ol"},"fire this command to spin up the docker compose:",(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"docker compose up \n")),"Hang on for a while as it installs all the required dependencies and starts all the required services. After a while we should see the output of the Dagger SQL query in the terminal, which will be the booking logs without any duplicate ",(0,n.kt)("inlineCode",{parentName:"li"},"order_number"),"."),(0,n.kt)("li",{parentName:"ol"},"fire this command to gracefully close the docker compose:",(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"docker compose down \n")),"This will stop and remove all the containers.")),(0,n.kt)("p",null,"Congratulations, we are now able to use Dagger to remove duplicate data from Kafka source!"))}c.isMDXComponent=!0}}]);