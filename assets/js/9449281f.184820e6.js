"use strict";(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[2607],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>g});var n=r(7294);function i(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function a(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?a(Object(r),!0).forEach((function(t){i(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):a(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,n,i=function(e,t){if(null==e)return{};var r,n,i={},a=Object.keys(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||(i[r]=e[r]);return i}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(n=0;n<a.length;n++)r=a[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(i[r]=e[r])}return i}var c=n.createContext({}),l=function(e){var t=n.useContext(c),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},u=function(e){var t=l(e.components);return n.createElement(c.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var r=e.components,i=e.mdxType,a=e.originalType,c=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),m=l(r),g=i,d=m["".concat(c,".").concat(g)]||m[g]||p[g]||a;return r?n.createElement(d,o(o({ref:t},u),{},{components:r})):n.createElement(d,o({ref:t},u))}));function g(e,t){var r=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var a=r.length,o=new Array(a);o[0]=m;var s={};for(var c in t)hasOwnProperty.call(t,c)&&(s[c]=t[c]);s.originalType=e,s.mdxType="string"==typeof e?e:i,o[1]=s;for(var l=2;l<a;l++)o[l]=r[l];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}m.displayName="MDXCreateElement"},607:(e,t,r)=>{r.r(t),r.d(t,{contentTitle:()=>o,default:()=>u,frontMatter:()=>a,metadata:()=>s,toc:()=>c});var n=r(7462),i=(r(7294),r(3905));const a={},o="Overview",s={unversionedId:"usecase/overview",id:"usecase/overview",isDocsHomePage:!1,title:"Overview",description:"This section talks a bit about some of the use cases that can be solved using Dagger with some examples.",source:"@site/docs/usecase/overview.md",sourceDirName:"usecase",slug:"/usecase/overview",permalink:"/dagger/docs/usecase/overview",editUrl:"https://github.com/odpf/dagger/edit/master/docs/docs/usecase/overview.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Security",permalink:"/dagger/docs/advance/security"},next:{title:"API Monitoring",permalink:"/dagger/docs/usecase/api_monitoring"}},c=[{value:"Stream Enrichment",id:"stream-enrichment",children:[]},{value:"Feature Ingestion",id:"feature-ingestion",children:[]},{value:"API Monitoring",id:"api-monitoring",children:[]}],l={toc:c};function u(e){let{components:t,...r}=e;return(0,i.kt)("wrapper",(0,n.Z)({},l,r,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"overview"},"Overview"),(0,i.kt)("p",null,"This section talks a bit about some of the use cases that can be solved using Dagger with some examples."),(0,i.kt)("h3",{id:"stream-enrichment"},(0,i.kt)("a",{parentName:"h3",href:"/dagger/docs/usecase/stream_enrichment"},"Stream Enrichment")),(0,i.kt)("p",null,"Enrichment of streaming Data with external more static Data sources is seamless in Dagger and is just writing a few configuration. We will explain this in more details with some example."),(0,i.kt)("h3",{id:"feature-ingestion"},(0,i.kt)("a",{parentName:"h3",href:"/dagger/docs/usecase/feature_ingestion"},"Feature Ingestion")),(0,i.kt)("p",null,"Realtime feature generation is really important for online machine learning pipelines. Dagger can be a great tool for feature ingestion."),(0,i.kt)("h3",{id:"api-monitoring"},(0,i.kt)("a",{parentName:"h3",href:"/dagger/docs/usecase/api_monitoring"},"API Monitoring")),(0,i.kt)("p",null,"Behind every great API is a reliable uptime monitoring system. But knowing/monitoring health of API metrics in real time is tricky. With Dagger this can be easily solved by writing some simple queries."))}u.isMDXComponent=!0}}]);