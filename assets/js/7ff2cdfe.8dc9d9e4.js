"use strict";(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[3641],{3905:(e,t,n)=>{n.d(t,{Zo:()=>c,kt:()=>m});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var s=r.createContext({}),d=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},c=function(e){var t=d(e.components);return r.createElement(s.Provider,{value:t},e.children)},u="mdxType",g={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},p=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,s=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),u=d(n),p=a,m=u["".concat(s,".").concat(p)]||u[p]||g[p]||o;return n?r.createElement(m,i(i({ref:t},c),{},{components:n})):r.createElement(m,i({ref:t},c))}));function m(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,i=new Array(o);i[0]=p;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[u]="string"==typeof e?e:a,i[1]=l;for(var d=2;d<o;d++)i[d]=n[d];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}p.displayName="MDXCreateElement"},5506:(e,t,n)=>{n.r(t),n.d(t,{contentTitle:()=>i,default:()=>c,frontMatter:()=>o,metadata:()=>l,toc:()=>s});var r=n(7462),a=(n(7294),n(3905));const o={},i="Development Guide",l={unversionedId:"contribute/development",id:"contribute/development",isDocsHomePage:!1,title:"Development Guide",description:"The following segment is a foundation for developing things on top of Dagger.",source:"@site/docs/contribute/development.md",sourceDirName:"contribute",slug:"/contribute/development",permalink:"/dagger/docs/contribute/development",editUrl:"https://github.com/odpf/dagger/edit/master/docs/docs/contribute/development.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Contribution Process",permalink:"/dagger/docs/contribute/contribution"},next:{title:"ADD Transformer",permalink:"/dagger/docs/contribute/add_transformer"}},s=[{value:"Run a Dagger",id:"run-a-dagger",children:[{value:"Development environment",id:"development-environment",children:[]},{value:"Services",id:"services",children:[]},{value:"Local Creation",id:"local-creation",children:[]}]},{value:"Style Guide",id:"style-guide",children:[{value:"Java",id:"java",children:[]}]},{value:"Code Modules",id:"code-modules",children:[]},{value:"Dependencies Configuration",id:"dependencies-configuration",children:[]},{value:"Integration Tests",id:"integration-tests",children:[{value:"Dev Setup for Local Integration Tests",id:"dev-setup-for-local-integration-tests",children:[]}]}],d={toc:s};function c(e){let{components:t,...n}=e;return(0,a.kt)("wrapper",(0,r.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"development-guide"},"Development Guide"),(0,a.kt)("p",null,"The following segment is a foundation for developing things on top of Dagger."),(0,a.kt)("p",null,"The main logical blocks of Dagger are:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"Streams"),": All Kafka source related information for Data consumption."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"SQL"),": SQL Query to process input stream data."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"Processors"),": Plugins to define custom Operators and to interact with external data sources."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"Sink"),": Sinking data after processing is done.")),(0,a.kt)("h2",{id:"run-a-dagger"},"Run a Dagger"),(0,a.kt)("h3",{id:"development-environment"},"Development environment"),(0,a.kt)("p",null,"The following environment is required for Dagger development"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Java SE Development Kit 8.")),(0,a.kt)("h3",{id:"services"},"Services"),(0,a.kt)("p",null,"The following components/services are required to run Dagger:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Kafka ",">"," 2.4 to consume messages from."),(0,a.kt)("li",{parentName:"ul"},"Corresponding sink service to sink data to."),(0,a.kt)("li",{parentName:"ul"},"Flink Cluster ","(","Optional",")"," only needed if you want to run in cluster mode. For standalone mode, it's not required.")),(0,a.kt)("h3",{id:"local-creation"},"Local Creation"),(0,a.kt)("p",null,"Follow ",(0,a.kt)("a",{parentName:"p",href:"/dagger/docs/guides/create_dagger"},"dagger creation guide")," for creating/configuring a Dagger in the local environment and to know more about the basic stages of Dagger."),(0,a.kt)("h2",{id:"style-guide"},"Style Guide"),(0,a.kt)("h3",{id:"java"},"Java"),(0,a.kt)("p",null,"We conform to the ",(0,a.kt)("a",{parentName:"p",href:"https://google.github.io/styleguide/javaguide.html"},"Google Java Style Guide"),". Maven can helpfully take care of that for you before you commit."),(0,a.kt)("h2",{id:"code-modules"},"Code Modules"),(0,a.kt)("p",null,"Dagger follows a multi-project build structure with multiple sub-projects. This allows us to maintain the code base logically segregated and remove the duplications. Describing briefly each submodule."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("strong",{parentName:"li"},(0,a.kt)("inlineCode",{parentName:"strong"},"dagger-core")),": The core module in Dagger. Accommodates All the core functionalities like SerDE, SQL execution, Processors and many more. Also efficiently interacts with other subprojects."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("strong",{parentName:"li"},(0,a.kt)("inlineCode",{parentName:"strong"},"dagger-common")),": This module contains all the code/contracts that are shared between other submodules. This allows reducing code duplicate and an efficient sharing of code between other submodules. For example, MetricsManagers are part of this submodule since metrics need to be recorded both in ",(0,a.kt)("inlineCode",{parentName:"li"},"dagger-common")," and ",(0,a.kt)("inlineCode",{parentName:"li"},"dagger-functions"),"."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("strong",{parentName:"li"},(0,a.kt)("inlineCode",{parentName:"strong"},"dagger-functions")),": Submodule that defines all the plugin components in Dagger like ",(0,a.kt)("a",{parentName:"li",href:"https://github.com/odpf/dagger/tree/main/dagger-functions/src/main/java/io/odpf/dagger/functions/udfs"},"UDFs")," and ",(0,a.kt)("a",{parentName:"li",href:"https://github.com/odpf/dagger/tree/main/dagger-functions/src/main/java/io/odpf/dagger/functions/transformers"},"Transformers"),"."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("strong",{parentName:"li"},(0,a.kt)("inlineCode",{parentName:"strong"},"dagger-tests")),": Integration test framework to test some central end to end flows in Dagger.")),(0,a.kt)("h2",{id:"dependencies-configuration"},"Dependencies Configuration"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Dagger generates three different types of jars. One traditional fat/shadow.jar with all the dagger specific code + the external dependencies, that is sufficient for running a dagger. Since this jar becomes heavyweight (~200MB) and takes few seconds to get uploaded to the cluster, Dagger provides another setup for jar creation where we segregate the dependencies from the Dagger code base."),(0,a.kt)("li",{parentName:"ul"},"The external dependencies in Dagger are less frequently updated. So we treat them as static and update on a major release."),(0,a.kt)("li",{parentName:"ul"},"The more frequently changing Dagger codebase reside in the minimal jar whose size is in KBs and gets uploaded to the cluster in a flash. The deployment section talks more about jar creation and how to deploy them to the Flink cluster."),(0,a.kt)("li",{parentName:"ul"},"The jar separation is a part of Gradle configuration. We define two new sets of configurations in all of the Dagger submodules.",(0,a.kt)("ul",{parentName:"li"},(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"minimalJar"),": add all dependencies to be included in the Dagger cluster user jar."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("inlineCode",{parentName:"li"},"dependenciesJar"),": add all dependencies to be included in the flink image.")))),(0,a.kt)("h2",{id:"integration-tests"},"Integration Tests"),(0,a.kt)("p",null,"Dagger interacts with external sources such as ElasticSearch, Postgres and HTTP endpoints to enrich the streams using post-processors. We have added Integration Tests to expose and resolve blunders in the interaction between integrated units at the time of development itself."),(0,a.kt)("p",null,"Integration Tests are part of the current CI set-up of Dagger and they must pass before releasing a version of Dagger. These will cover all the flows of postprocessors with different external sources and ensure a bug-free dagger."),(0,a.kt)("h3",{id:"dev-setup-for-local-integration-tests"},"Dev Setup for Local Integration Tests"),(0,a.kt)("h4",{id:"setup-elasticsearch"},"Setup Elasticsearch"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"\n# install elastic search\n\nbrew update\nbrew install elasticsearch\nbrew services start elasticsearch\n\n")),(0,a.kt)("h4",{id:"setup-postgres"},"Setup Postgres"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"# install postgres in local environment\nbrew update\nbrew install postgresql\n#configure postgres for integration tests\npsql postgres\n>> create user root with password root\n>> create ROLE root WITH LOGIN PASSWORD 'root' CREATEDB;\n# Login Via root now and create test database\n>> psql postgres -U root\n>> postgres=>create database test_db;\n")),(0,a.kt)("h4",{id:"run-integration-tests"},"Run Integration tests"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"./gradlew clean IntegrationTest\n")))}c.isMDXComponent=!0}}]);