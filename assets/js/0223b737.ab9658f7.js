"use strict";(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[5530],{3905:(e,r,t)=>{t.d(r,{Zo:()=>c,kt:()=>f});var a=t(7294);function n(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function o(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);r&&(a=a.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,a)}return t}function s(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?o(Object(t),!0).forEach((function(r){n(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):o(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function i(e,r){if(null==e)return{};var t,a,n=function(e,r){if(null==e)return{};var t,a,n={},o=Object.keys(e);for(a=0;a<o.length;a++)t=o[a],r.indexOf(t)>=0||(n[t]=e[t]);return n}(e,r);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)t=o[a],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(n[t]=e[t])}return n}var m=a.createContext({}),p=function(e){var r=a.useContext(m),t=r;return e&&(t="function"==typeof e?e(r):s(s({},r),e)),t},c=function(e){var r=p(e.components);return a.createElement(m.Provider,{value:r},e.children)},l={inlineCode:"code",wrapper:function(e){var r=e.children;return a.createElement(a.Fragment,{},r)}},d=a.forwardRef((function(e,r){var t=e.components,n=e.mdxType,o=e.originalType,m=e.parentName,c=i(e,["components","mdxType","originalType","parentName"]),d=p(t),f=n,u=d["".concat(m,".").concat(f)]||d[f]||l[f]||o;return t?a.createElement(u,s(s({ref:r},c),{},{components:t})):a.createElement(u,s({ref:r},c))}));function f(e,r){var t=arguments,n=r&&r.mdxType;if("string"==typeof e||n){var o=t.length,s=new Array(o);s[0]=d;var i={};for(var m in r)hasOwnProperty.call(r,m)&&(i[m]=r[m]);i.originalType=e,i.mdxType="string"==typeof e?e:n,s[1]=i;for(var p=2;p<o;p++)s[p]=t[p];return a.createElement.apply(null,s)}return a.createElement.apply(null,t)}d.displayName="MDXCreateElement"},5249:(e,r,t)=>{t.r(r),t.d(r,{contentTitle:()=>s,default:()=>c,frontMatter:()=>o,metadata:()=>i,toc:()=>m});var a=t(7462),n=(t(7294),t(3905));const o={},s="ADD Transformer",i={unversionedId:"contribute/add_transformer",id:"contribute/add_transformer",isDocsHomePage:!1,title:"ADD Transformer",description:"The Transformers give Dagger the capability for applying any custom transformation logic user wants on Data(pre or post aggregated). In simple terms, Transformers are user-defined Java code that can do transformations like Map, Filter, Flat maps etc. Find more information on Transformers here.",source:"@site/docs/contribute/add_transformer.md",sourceDirName:"contribute",slug:"/contribute/add_transformer",permalink:"/dagger/docs/contribute/add_transformer",editUrl:"https://github.com/odpf/dagger/edit/master/docs/docs/contribute/add_transformer.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Development Guide",permalink:"/dagger/docs/contribute/development"},next:{title:"ADD UDF",permalink:"/dagger/docs/contribute/add_udf"}},m=[],p={toc:m};function c(e){let{components:r,...t}=e;return(0,n.kt)("wrapper",(0,a.Z)({},p,t,{components:r,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"add-transformer"},"ADD Transformer"),(0,n.kt)("p",null,"The Transformers give Dagger the capability for applying any custom ",(0,n.kt)("em",{parentName:"p"},"transformation logic")," user wants on Data(pre or post aggregated). In simple terms, Transformers are user-defined Java code that can do transformations like Map, Filter, Flat maps etc. Find more information on Transformers here."),(0,n.kt)("p",null,"Many Transformation logics are pre-supported in Dagger. But since Transformers are more advanced ways of injecting business logic as plugins to Dagger, there can be cases when existing Transformers are not sufficient for user requirement. This section documents how you can add Transformers to dagger."),(0,n.kt)("p",null,"For adding custom Transformers follow these steps"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Ensure none of the ",(0,n.kt)("a",{parentName:"p",href:"/dagger/docs/reference/transformers"},"built-in Transformers")," suits your requirement.")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Transformers take ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/odpf/dagger/blob/main/dagger-common/src/main/java/io/odpf/dagger/common/core/StreamInfo.java"},"StreamInfo")," which is a wrapper around Flink DataStream as input and transform them to some other StreamInfo/DataStream.")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"To define a new Transformer implement Transformer interface. The contract of Transformers is defined ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/odpf/dagger/blob/main/dagger-common/src/main/java/io/odpf/dagger/common/core/Transformer.java"},"here"),".")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Since an input DataStream is available in Transformer, all the Flink supported operators which transform ",(0,n.kt)("inlineCode",{parentName:"p"},"DataStream -> DataStream")," can be applied/used by default for the transformations. Operators are how Flink exposes classic Map-reduce type functionalities. Read more about Flink Operators ",(0,n.kt)("a",{parentName:"p",href:"https://ci.apache.org/projects/flink/flink-docs-release-1.14/dev/stream/operators/"},"here"),".")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"In the case of single Operator Transformation you can extend the desired Operator in the Transformer class itself. For example, follow this code of ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/odpf/dagger/blob/main/dagger-functions/src/main/java/io/odpf/dagger/functions/transformers/HashTransformer.java"},"HashTransformer"),". You can also define multiple chaining operators to Transform Data.")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"A configuration ",(0,n.kt)("inlineCode",{parentName:"p"},"transformation_arguments")," inject the required parameters as a Constructor argument to the Transformer class. From the config point of view, these are simple Map of String and Object. So you need to cast them to your desired data types. Find a more detailed overview of the transformer example ",(0,n.kt)("a",{parentName:"p",href:"/dagger/docs/guides/use_transformer"},"here"),".")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Transformers are injected into the ",(0,n.kt)("inlineCode",{parentName:"p"},"dagger-core")," during runtime using Java reflection. So unlike UDFs, they don't need registration. You just need to mention the fully qualified Transformer Java class Name in the configurations.")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Bump up the version and raise a PR for the Transformer. Also please add the Transformer to the ",(0,n.kt)("a",{parentName:"p",href:"/dagger/docs/reference/transformers"},"list of Transformers doc"),". Once the PR gets merged the transformer should be available to be used.")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"If you have specific use-cases you are solving using Transformers and you don't want to add them to the open-source repo, you can have a separate local codebase for those Transformers and add it to the classpath of the dagger. With the correct Transformation configurations, they should be available to use out of the box."))),(0,n.kt)("p",null,(0,n.kt)("inlineCode",{parentName:"p"},"Note"),": ",(0,n.kt)("em",{parentName:"p"},"Please go through the ",(0,n.kt)("a",{parentName:"em",href:"/dagger/docs/contribute/contribution"},"Contribution guide")," to know about all the conventions and practices we tend to follow and to know about the contribution process to the dagger.")))}c.isMDXComponent=!0}}]);