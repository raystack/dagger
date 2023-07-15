"use strict";(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[2898],{3905:(e,r,t)=>{t.d(r,{Zo:()=>c,kt:()=>f});var a=t(7294);function n(e,r,t){return r in e?Object.defineProperty(e,r,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[r]=t,e}function o(e,r){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);r&&(a=a.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),t.push.apply(t,a)}return t}function s(e){for(var r=1;r<arguments.length;r++){var t=null!=arguments[r]?arguments[r]:{};r%2?o(Object(t),!0).forEach((function(r){n(e,r,t[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):o(Object(t)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(t,r))}))}return e}function i(e,r){if(null==e)return{};var t,a,n=function(e,r){if(null==e)return{};var t,a,n={},o=Object.keys(e);for(a=0;a<o.length;a++)t=o[a],r.indexOf(t)>=0||(n[t]=e[t]);return n}(e,r);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)t=o[a],r.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(n[t]=e[t])}return n}var l=a.createContext({}),p=function(e){var r=a.useContext(l),t=r;return e&&(t="function"==typeof e?e(r):s(s({},r),e)),t},c=function(e){var r=p(e.components);return a.createElement(l.Provider,{value:r},e.children)},m="mdxType",u={inlineCode:"code",wrapper:function(e){var r=e.children;return a.createElement(a.Fragment,{},r)}},d=a.forwardRef((function(e,r){var t=e.components,n=e.mdxType,o=e.originalType,l=e.parentName,c=i(e,["components","mdxType","originalType","parentName"]),m=p(t),d=n,f=m["".concat(l,".").concat(d)]||m[d]||u[d]||o;return t?a.createElement(f,s(s({ref:r},c),{},{components:t})):a.createElement(f,s({ref:r},c))}));function f(e,r){var t=arguments,n=r&&r.mdxType;if("string"==typeof e||n){var o=t.length,s=new Array(o);s[0]=d;var i={};for(var l in r)hasOwnProperty.call(r,l)&&(i[l]=r[l]);i.originalType=e,i[m]="string"==typeof e?e:n,s[1]=i;for(var p=2;p<o;p++)s[p]=t[p];return a.createElement.apply(null,s)}return a.createElement.apply(null,t)}d.displayName="MDXCreateElement"},6671:(e,r,t)=>{t.r(r),t.d(r,{contentTitle:()=>s,default:()=>m,frontMatter:()=>o,metadata:()=>i,toc:()=>l});var a=t(7462),n=(t(7294),t(3905));const o={},s="Use Transformer",i={unversionedId:"guides/use_transformer",id:"guides/use_transformer",isDocsHomePage:!1,title:"Use Transformer",description:"Dagger exposes a configuration + code driven framework for defining complex Map-Reduce type Flink code which enables dagger to process data beyond SQL. We call them Transformers.",source:"@site/docs/guides/use_transformer.md",sourceDirName:"guides",slug:"/guides/use_transformer",permalink:"/dagger/docs/guides/use_transformer",editUrl:"https://github.com/raystack/dagger/edit/master/docs/docs/guides/use_transformer.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Example Queries",permalink:"/dagger/docs/guides/query_examples"},next:{title:"Use UDF",permalink:"/dagger/docs/guides/use_udf"}},l=[{value:"What is Transformer",id:"what-is-transformer",children:[]},{value:"Explore Pre-built Transformer",id:"explore-pre-built-transformer",children:[]},{value:"Use a Transformer",id:"use-a-transformer",children:[]}],p={toc:l},c="wrapper";function m(e){let{components:r,...t}=e;return(0,n.kt)(c,(0,a.Z)({},p,t,{components:r,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"use-transformer"},"Use Transformer"),(0,n.kt)("p",null,"Dagger exposes a ",(0,n.kt)("inlineCode",{parentName:"p"},"configuration + code")," driven framework for defining complex Map-Reduce type Flink code which enables dagger to process data beyond SQL. We call them Transformers.\nIn this section, we will know more about transformers, how to use them and how you can create your transformers matching your use case."),(0,n.kt)("h2",{id:"what-is-transformer"},"What is Transformer"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"During stream processing there are only so many things you can do using SQLs and after a while, it becomes a bottleneck to solve complex problems. Though dagger is developed keeping SQL first philosophy in mind we realized the necessity to expose some of the complex flink features like async processing, ability to write custom operator(classic Map-Reduce type functions) etc.")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"We developed a configuration driven framework called processors which is another way of processing data in addition to SQL. Processors are extensible and can be applied on the stream before SQL execution (called pre-processor; ideal for complex filtration) or after SQL execution (called post-processor; ideal for async external processing and complex aggregations).")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Transformers are a type of processors that let users define more complex processing capability by writing custom Java code. With transformers, all the ",(0,n.kt)("a",{parentName:"p",href:"https://ci.apache.org/projects/flink/flink-docs-master/docs/dev/datastream/operators/overview/"},"Flink Operators")," and granularity of ",(0,n.kt)("a",{parentName:"p",href:"https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/datastream/operators/process_function/"},"Flink process Function")," are supported out of the box. This lets users solve some of the more complex business-specific problems.")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"There can be multiple transformers chained as well. They will be processed sequentially.")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Transformer is a single-stream operation transforming one stream to other. So there are some inherent limitations like multi-stream operations in transformations."))),(0,n.kt)("h2",{id:"explore-pre-built-transformer"},"Explore Pre-built Transformer"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"There are some transformers to solve some generic use cases pre-built in the dagger.")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"All the pre-supported transformers present in the ",(0,n.kt)("inlineCode",{parentName:"p"},"dagger-functions")," sub-module in ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/raystack/dagger/tree/main/dagger-functions/src/main/java/org/raystack/dagger/functions/transformers"},"this")," directory. Find more details about each of the existing transformers and some sample examples ",(0,n.kt)("a",{parentName:"p",href:"/dagger/docs/reference/transformers"},"here"),".")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"In case any of the predefined transformers do not meet your requirement, you can create your custom Transformers by extending some contract. Follow this ",(0,n.kt)("a",{parentName:"p",href:"/dagger/docs/contribute/add_transformer"},"contribution guidelines")," on how to add a transformer in dagger."))),(0,n.kt)("h2",{id:"use-a-transformer"},"Use a Transformer"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"Below is a sample query and post-processor configuration for a dagger that uses transformers. As you can see transformers need to be a part of the processor (more specifically Postprocessor in this example)."),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-properties"},'SQL_QUERY = "SELECT data_1, data_2, event_timestamp from data_stream"\nPOST_PROCESSOR_ENABLED = true\nPOST_PROCESSOR_CONFIG = {\n  "internal_source": [\n      {\n          "output_field": "*",\n          "value": "*",\n          "type": "sql"\n      }\n  ],\n  "transformers": [\n      {\n          "transformation_class": "HashTransformer",\n          "transformation_arguments": {\n              "maskColumns": [\n                  "data_2",\n                  "data_1"\n              ]\n          }\n      }\n  ]\n}\n'))),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"In the example, the internal source just says to select all the fields as selected from the SQL query. Find more about the ",(0,n.kt)("inlineCode",{parentName:"p"},"internal_source")," config parameter ",(0,n.kt)("a",{parentName:"p",href:"/dagger/docs/advance/post_processor#internal-post-processor"},"here"),".")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"The transformer essentially need only a couple of config parameters to work. Provide the fully qualified path of the transformer class as part of the ",(0,n.kt)("inlineCode",{parentName:"p"},"transformation_class")," config.")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"The other parameter ",(0,n.kt)("inlineCode",{parentName:"p"},"transformation_arguments")," is a map of string and data types where you can put the parameters to be passed to the transformer class as a key-value pair.")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("p",{parentName:"li"},"For more information about all existing transformers and their functionalities have a look ",(0,n.kt)("a",{parentName:"p",href:"/dagger/docs/reference/transformers"},"here"),"."))))}m.isMDXComponent=!0}}]);