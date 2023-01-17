"use strict";(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[366],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>g});var o=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function r(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);t&&(o=o.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,o)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?r(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,o,a=function(e,t){if(null==e)return{};var n,o,a={},r=Object.keys(e);for(o=0;o<r.length;o++)n=r[o],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(o=0;o<r.length;o++)n=r[o],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var s=o.createContext({}),d=function(e){var t=o.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=d(e.components);return o.createElement(s.Provider,{value:t},e.children)},u="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return o.createElement(o.Fragment,{},t)}},c=o.forwardRef((function(e,t){var n=e.components,a=e.mdxType,r=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),u=d(n),c=a,g=u["".concat(s,".").concat(c)]||u[c]||m[c]||r;return n?o.createElement(g,i(i({ref:t},p),{},{components:n})):o.createElement(g,i({ref:t},p))}));function g(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var r=n.length,i=new Array(r);i[0]=c;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[u]="string"==typeof e?e:a,i[1]=l;for(var d=2;d<r;d++)i[d]=n[d];return o.createElement.apply(null,i)}return o.createElement.apply(null,n)}c.displayName="MDXCreateElement"},2433:(e,t,n)=>{n.r(t),n.d(t,{contentTitle:()=>i,default:()=>p,frontMatter:()=>r,metadata:()=>l,toc:()=>s});var o=n(7462),a=(n(7294),n(3905));const r={},i="Longbow",l={unversionedId:"advance/longbow",id:"advance/longbow",isDocsHomePage:!1,title:"Longbow",description:"This is another type of processor which is also applied post SQL query processing in the Dagger workflow. This feature allows users to aggregate data over long windows in real-time. For certain use-cases, you need to know the historical data for an event in a given context. Eg: For a booking event, a risk prevention system would be interested in 1 month pattern of the customer.",source:"@site/docs/advance/longbow.md",sourceDirName:"advance",slug:"/advance/longbow",permalink:"/dagger/docs/advance/longbow",editUrl:"https://github.com/odpf/dagger/edit/master/docs/docs/advance/longbow.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Post Processors",permalink:"/dagger/docs/advance/post_processor"},next:{title:"Longbow+",permalink:"/dagger/docs/advance/longbow_plus"}},s=[{value:"Longbow writer",id:"longbow-writer",children:[{value:"Workflow",id:"workflow",children:[]}]},{value:"Longbow reader",id:"longbow-reader",children:[{value:"Workflow",id:"workflow-1",children:[]},{value:"<code>longbow_key</code>",id:"longbow_key",children:[]},{value:"<code>longbow_duration</code>",id:"longbow_duration",children:[]},{value:"<code>longbow_latest</code>",id:"longbow_latest",children:[]},{value:"<code>longbow_earliest</code>",id:"longbow_earliest",children:[]},{value:"<code>event_timestamp</code>",id:"event_timestamp",children:[]},{value:"<code>rowtime</code>",id:"rowtime",children:[]}]}],d={toc:s};function p(e){let{components:t,...r}=e;return(0,a.kt)("wrapper",(0,o.Z)({},d,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"longbow"},"Longbow"),(0,a.kt)("p",null,"This is another type of processor which is also applied post SQL query processing in the Dagger workflow. This feature allows users to aggregate data over long windows in real-time. For certain use-cases, you need to know the historical data for an event in a given context. Eg: For a booking event, a risk prevention system would be interested in 1 month pattern of the customer.\nLongbow solves the above problem, the entire historical context gets added in the same event which allows downstream systems to process data without any external dependency. In order to achieve this, we store the historical data in an external data source. After evaluating a lot of data sources we found ",(0,a.kt)("a",{parentName:"p",href:"https://cloud.google.com/bigtable"},"Bigtable")," to be a good fit primarily because of its low scan queries latencies. This currently works only for Kafka sink."),(0,a.kt)("h1",{id:"components"},"Components"),(0,a.kt)("p",null,"In order to make this work in a single Dagger job, we created the following components."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"/dagger/docs/advance/longbow#longbow-writer"},"Longbow writer")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"/dagger/docs/advance/longbow#longbow-reader"},"Longbow reader"))),(0,a.kt)("h2",{id:"longbow-writer"},"Longbow writer"),(0,a.kt)("p",null,"This component is responsible for writing the latest data to Bigtable. It uses Flink's ",(0,a.kt)("a",{parentName:"p",href:"https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/asyncio.html"},"Async IO")," in order to make this network call."),(0,a.kt)("h3",{id:"workflow"},"Workflow"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Create a new table(if doesn't exist) with the name same as Dagger job name or using ",(0,a.kt)("a",{parentName:"li",href:"/dagger/docs/reference/configuration#processor_longbow_gcp_project_id"},"PROCESSOR_LONGBOW_GCP_PROJECT_ID"),"."),(0,a.kt)("li",{parentName:"ul"},"Receives the record post SQL query processing."),(0,a.kt)("li",{parentName:"ul"},"Creates the Bigtable key by combining data from longbow_key, a delimiter, and reversing the event_timestamp. Timestamps are reversed in order to achieve lower latencies in scan query, more details ",(0,a.kt)("a",{parentName:"li",href:"https://cloud.google.com/bigtable/docs/schema-design#time-based"},"here"),"."),(0,a.kt)("li",{parentName:"ul"},"Creates the request by adding all the column values from SQL as Bigtable row columns which are passed with ",(0,a.kt)("inlineCode",{parentName:"li"},"longbow_data")," as a substring in the column name."),(0,a.kt)("li",{parentName:"ul"},"Makes the request."),(0,a.kt)("li",{parentName:"ul"},"Passes the original record post SQL without any modifications to longbow_reader.")),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Note:")," By default we create tables with retention of 3 months in Bigtable."),(0,a.kt)("h2",{id:"longbow-reader"},"Longbow reader"),(0,a.kt)("p",null,"This component is responsible for reading the historical data from Bigtable and forwarding it to the sink. It also uses Flink's ",(0,a.kt)("a",{parentName:"p",href:"https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/operators/asyncio.html"},"Async IO")," in order to make this network call."),(0,a.kt)("h3",{id:"workflow-1"},"Workflow"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Builds the scan request for Bigtable. We provide two configurable strategies for defining the range of the query.",(0,a.kt)("ul",{parentName:"li"},(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("strong",{parentName:"li"},"Duration range:")," It will scan from the latest event's timestamp to a provided duration."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("strong",{parentName:"li"},"Absolute range:")," You can provide the absolute range by setting ",(0,a.kt)("a",{parentName:"li",href:"/dagger/docs/advance/longbow#longbow_latest"},"longbow_latest")," and ",(0,a.kt)("a",{parentName:"li",href:"/dagger/docs/advance/longbow#longbow_earliest"},"longbow_earliest")," in the SQL query."))),(0,a.kt)("li",{parentName:"ul"},"Makes the scan request and receives the result."),(0,a.kt)("li",{parentName:"ul"},"Parses the response and creates a separate list of values for every column. Thus every record post this stage will have its historical data within the same record."),(0,a.kt)("li",{parentName:"ul"},"Forwards the data to the sink.")),(0,a.kt)("h1",{id:"data-flow-in-longbow"},"Data flow in longbow"),(0,a.kt)("p",null,"In this example, let's assume we have booking events in a Kafka cluster and we want to get information of all the order numbers and their driver ids for customers in the last 30 days. Here customer_id will become longbow_key."),(0,a.kt)("p",null,(0,a.kt)("img",{src:n(2564).Z})),(0,a.kt)("p",null,"Sample input schema for booking"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-protobuf"},"message SampleBookingInfo {\n  string order_number = 1;\n  string order_url = 2;\n  Status.Enum status = 3;\n  google.protobuf.Timestamp event_timestamp = 4;\n  string customer_id = 5;\n  string driver_id = 6;\n}\n")),(0,a.kt)("p",null,"Sample output schema for longbow output"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-protobuf"},"message BookingWithHistory {\n  string customer_id = 1;\n  google.protobuf.Timestamp event_timestamp = 2;\n  repeated string longbow_data1 = 3;\n  repeated string longbow_data2 = 4;\n  string longbow_duration = 5;\n}\n")),(0,a.kt)("p",null,"Sample Query"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-SQL"},"# here booking denotes the booking events stream with the sample input schema\nSELECT\n  CURRENT_TIMESTAMP AS event_timestamp,\n  customer_id AS longbow_key,\n  customer_id AS customer_id,\n  order_number AS longbow_data1,\n  driver_id AS longbow_data2,\n  '30d' AS longbow_duration,\n  rowtime AS rowtime\nFROM\n  booking\n")),(0,a.kt)("p",null,"In the above example, order_number and driver_id fields are selected as ",(0,a.kt)("inlineCode",{parentName:"p"},"longbow_data1")," and ",(0,a.kt)("inlineCode",{parentName:"p"},"longbow_data2"),", so they will be considered as Bigtable row columns as explained in the longbow writer workflow above. The final output will have a list of order_numbers in the ",(0,a.kt)("inlineCode",{parentName:"p"},"longbow_data1")," field and a list of driver_id in ",(0,a.kt)("inlineCode",{parentName:"p"},"longbow_data2"),"."),(0,a.kt)("p",null,"In case you want orders from the start of the month to the end of the month, then instead of using the Duration range i.e. 30d, you can also give an absolute range, example query below."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-SQL"},"# here booking denotes the booking events stream with the sample input schema\nSELECT\n  CURRENT_TIMESTAMP AS event_timestamp,\n  customer_id AS longbow_key,\n  customer_id AS customer_id,\n  order_number AS longbow_data1,\n  driver_id AS longbow_data2,\n  StartOfMonth(\n    TimestampFromUnix(event_timestamp.seconds),\n    'Asia/Jakarta'\n  ) as longbow_earliest,\n  EndOfMonth(\n    TimestampFromUnix(event_timestamp.seconds),\n    'Asia/Jakarta'\n  ) as longbow_latest,\n  rowtime AS rowtime\nFROM\n  booking\n")),(0,a.kt)("p",null,"Here, ",(0,a.kt)("a",{parentName:"p",href:"/dagger/docs/reference/udfs#startofmonth"},"StartOfMonth"),", ",(0,a.kt)("a",{parentName:"p",href:"/dagger/docs/reference/udfs#endofmonth"},"EndOfMonth")," and ",(0,a.kt)("a",{parentName:"p",href:"/dagger/docs/reference/udfs#timestampfromunix"},"TimestampFromUnix")," are custom UDFs."),(0,a.kt)("h1",{id:"configurations"},"Configurations"),(0,a.kt)("p",null,"Longbow is entirely driven via SQL query, i.e. on the basis of presence of certain columns we identify longbow parameters. Following configs should be passed via SQL query as shown in the above example."),(0,a.kt)("h3",{id:"longbow_key"},(0,a.kt)("inlineCode",{parentName:"h3"},"longbow_key")),(0,a.kt)("p",null,"The key from the input which should be used to create the row key for Bigtable. Longbow will be enabled only if this column is present."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"customer_id")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required"))),(0,a.kt)("h3",{id:"longbow_duration"},(0,a.kt)("inlineCode",{parentName:"h3"},"longbow_duration")),(0,a.kt)("p",null,"The duration for the scan query. It can be passed in minutes(m), hours(h) and days(d)."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"30d")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"optional"))),(0,a.kt)("h3",{id:"longbow_latest"},(0,a.kt)("inlineCode",{parentName:"h3"},"longbow_latest")),(0,a.kt)("p",null,"The latest/recent value for the range of scan. This needs to be set only in case of absolute range. You can also use custom UDFs to generate this value as shown in the above example."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"1625097599")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"optional"))),(0,a.kt)("h3",{id:"longbow_earliest"},(0,a.kt)("inlineCode",{parentName:"h3"},"longbow_earliest")),(0,a.kt)("p",null,"The earliest/oldest value for the range of scan. This needs to be set only in case of absolute range. You can also use custom UDFs to generate this value as shown in the above example."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"1622505600")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"optional"))),(0,a.kt)("h3",{id:"event_timestamp"},(0,a.kt)("inlineCode",{parentName:"h3"},"event_timestamp")),(0,a.kt)("p",null,"The timestamp to be used to build the Bigtable row keys."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"CURRENT_TIMESTAMP")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required"))),(0,a.kt)("h3",{id:"rowtime"},(0,a.kt)("inlineCode",{parentName:"h3"},"rowtime")),(0,a.kt)("p",null,"The time attribute column. Read more ",(0,a.kt)("a",{parentName:"p",href:"/dagger/docs/concepts/basics#rowtime"},"here"),"."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Example value: ",(0,a.kt)("inlineCode",{parentName:"li"},"rowtime")),(0,a.kt)("li",{parentName:"ul"},"Type: ",(0,a.kt)("inlineCode",{parentName:"li"},"required"))),(0,a.kt)("p",null,"For additional global configs regarding longbow, please refer ",(0,a.kt)("a",{parentName:"p",href:"/dagger/docs/reference/configuration#longbow"},"here"),"."))}p.isMDXComponent=!0},2564:(e,t,n)=>{n.d(t,{Z:()=>o});const o=n.p+"assets/images/longbow-d5ab62f3a8576e5162ea8fd9acc2fc90.png"}}]);