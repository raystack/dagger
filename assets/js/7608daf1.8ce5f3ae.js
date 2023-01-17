"use strict";(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[8497],{3905:(e,t,a)=>{a.d(t,{Zo:()=>u,kt:()=>c});var n=a(7294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function l(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function o(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?l(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):l(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function i(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},l=Object.keys(e);for(n=0;n<l.length;n++)a=l[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(n=0;n<l.length;n++)a=l[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),m=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):o(o({},t),e)),a},u=function(e){var t=m(e.components);return n.createElement(s.Provider,{value:t},e.children)},p="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},f=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,l=e.originalType,s=e.parentName,u=i(e,["components","mdxType","originalType","parentName"]),p=m(a),f=r,c=p["".concat(s,".").concat(f)]||p[f]||d[f]||l;return a?n.createElement(c,o(o({ref:t},u),{},{components:a})):n.createElement(c,o({ref:t},u))}));function c(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var l=a.length,o=new Array(l);o[0]=f;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i[p]="string"==typeof e?e:r,o[1]=i;for(var m=2;m<l;m++)o[m]=a[m];return n.createElement.apply(null,o)}return n.createElement.apply(null,a)}f.displayName="MDXCreateElement"},6592:(e,t,a)=>{a.r(t),a.d(t,{contentTitle:()=>o,default:()=>u,frontMatter:()=>l,metadata:()=>i,toc:()=>s});var n=a(7462),r=(a(7294),a(3905));const l={},o="Transformers",i={unversionedId:"reference/transformers",id:"reference/transformers",isDocsHomePage:!1,title:"Transformers",description:"This page contains references for all the custom transformers available on Dagger.",source:"@site/docs/reference/transformers.md",sourceDirName:"reference",slug:"/reference/transformers",permalink:"/dagger/docs/reference/transformers",editUrl:"https://github.com/odpf/dagger/edit/master/docs/docs/reference/transformers.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Metrics",permalink:"/dagger/docs/reference/metrics"},next:{title:"Udfs",permalink:"/dagger/docs/reference/udfs"}},s=[{value:"List of Transformers",id:"list-of-transformers",children:[{value:"ClearColumnTransformer",id:"clearcolumntransformer",children:[]},{value:"DeDuplicationTransformer",id:"deduplicationtransformer",children:[]},{value:"FeatureTransformer",id:"featuretransformer",children:[]},{value:"FeatureWithTypeTransformer",id:"featurewithtypetransformer",children:[]},{value:"HashTransformer",id:"hashtransformer",children:[]},{value:"InvalidRecordFilterTransformer",id:"invalidrecordfiltertransformer",children:[]},{value:"SQLTransformer",id:"sqltransformer",children:[]}]}],m={toc:s};function u(e){let{components:t,...a}=e;return(0,r.kt)("wrapper",(0,n.Z)({},m,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"transformers"},"Transformers"),(0,r.kt)("p",null,"This page contains references for all the custom transformers available on Dagger."),(0,r.kt)("h2",{id:"list-of-transformers"},"List of Transformers"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"/dagger/docs/reference/transformers#ClearColumnTransformer"},"ClearColumnTransformer")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"/dagger/docs/reference/transformers#DeDuplicationTransformer"},"DeDuplicationTransformer")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"/dagger/docs/reference/transformers#FeatureTransformer"},"FeatureTransformer")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"/dagger/docs/reference/transformers#FeatureWithTypeTransformer"},"FeatureWithTypeTransformer")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"/dagger/docs/reference/transformers#HashTransformer"},"HashTransformer")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"/dagger/docs/reference/transformers#InvalidRecordFilterTransformer"},"InvalidRecordFilterTransformer")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"/dagger/docs/reference/transformers#SQLTransformer"},"SQLTransformer"))),(0,r.kt)("h3",{id:"clearcolumntransformer"},"ClearColumnTransformer"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Transformation Class:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"io.odpf.dagger.functions.transformers.ClearColumnTransformer")))),(0,r.kt)("li",{parentName:"ul"},"Contract: ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"After Selecting columns by SQL, you need to reselect the desired columns with the help of an internal source. Following transformation arguments can be passed:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"targetColumnName"),": The field that needs to be cleared."))))),(0,r.kt)("li",{parentName:"ul"},"Functionality:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Allows clearing the specified column of data produced by the dagger."),(0,r.kt)("li",{parentName:"ul"},"Can be used only on ",(0,r.kt)("inlineCode",{parentName:"li"},"post-processor")))),(0,r.kt)("li",{parentName:"ul"},"Example:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"SQL:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},"SELECT\n  event_timestamp,\n  data1,\n  data2\nFROM\n  data_stream\n"))),(0,r.kt)("li",{parentName:"ul"},"POST PROCESSOR CONFIG:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},'{\n  "internal_source": [\n    {\n      "output_field": "*",\n      "value": "*",\n      "type": "sql"\n    }\n  ],\n  "transformers": [\n    {\n      "transformation_class": "io.odpf.dagger.functions.transformers.ClearColumnTransformer",\n      "transformation_arguments": {\n        "targetColumnName": "data1"\n      }\n    }\n  ]\n}\n')))))),(0,r.kt)("h3",{id:"deduplicationtransformer"},"DeDuplicationTransformer"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Transformation Class:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"io.odpf.dagger.functions.transformers.DeDuplicationTransformer")))),(0,r.kt)("li",{parentName:"ul"},"Contract: ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"After Selecting columns by SQL, you need to reselect the desired columns with the help of an internal source. Following transformation arguments can be passed:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"key_column"),": This value will be used as the deduplication key (other events with the same key will be stopped). "),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"ttl_in_seconds"),": The TTL configuration will decide how long to keep the keys in memory. Once the keys are cleared from memory the data with the same keys will be sent again."))))),(0,r.kt)("li",{parentName:"ul"},"Functionality:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Allows deduplication of data produced by the dagger i.e records with the same key will not be sent again till the TTL expires."),(0,r.kt)("li",{parentName:"ul"},"Can be used both on ",(0,r.kt)("inlineCode",{parentName:"li"},"post-processor")," and ",(0,r.kt)("inlineCode",{parentName:"li"},"pre-processor")))),(0,r.kt)("li",{parentName:"ul"},"Example:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"SQL:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},"SELECT\n  data1,\n  data2\nFROM\n  data_stream\n"))),(0,r.kt)("li",{parentName:"ul"},"POST PROCESSOR CONFIG:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},'{\n  "internal_source": [\n    {\n      "output_field": "data1",\n      "value": "data1",\n      "type": "sql"\n    },\n    {\n      "output_field": "data2",\n      "value": "data2",\n      "type": "sql"\n    }\n  ],\n  "transformers": [\n    {\n      "transformation_arguments": {\n        "key_column": "data1",\n        "ttl_in_seconds": "3600"\n      },\n      "transformation_class": "io.odpf.dagger.functions.transformers.DeDuplicationTransformer"\n    }\n  ]\n}\n')))))),(0,r.kt)("h3",{id:"featuretransformer"},"FeatureTransformer"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Transformation Class:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"io.odpf.dagger.functions.transformers.FeatureTransformer")))),(0,r.kt)("li",{parentName:"ul"},"Contract: ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"After Selecting columns by SQL, you need to reselect the desired columns with the help of an internal source. Following transformation arguments can be passed:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"keyColumnName"),": This value will be used to form the key of the feature. "),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"valueColumnName"),": This value will be used as a value in the feature."))))),(0,r.kt)("li",{parentName:"ul"},"Functionality:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Converts to feast Features from post processors."),(0,r.kt)("li",{parentName:"ul"},"Can be used only on ",(0,r.kt)("inlineCode",{parentName:"li"},"post-processor")))),(0,r.kt)("li",{parentName:"ul"},"Example:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"SQL:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},"SELECT \n  data1, \n  features \nFROM\n  data_stream\n"))),(0,r.kt)("li",{parentName:"ul"},"POST PROCESSOR CONFIG:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},'{\n  "internal_source": [\n    {\n      "output_field": "*",\n      "value": "*",\n      "type": "sql"\n    }\n  ],\n  "transformers": [\n    {\n      "transformation_arguments": {\n        "keyColumnName": "data1",\n        "valueColumnName": "features"\n      },\n      "transformation_class": "io.odpf.dagger.functions.transformers.FeatureTransformer"\n    }\n  ]\n}\n')))))),(0,r.kt)("h3",{id:"featurewithtypetransformer"},"FeatureWithTypeTransformer"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Transformation Class:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"io.odpf.dagger.functions.transformers.FeatureWithTypeTransformer")))),(0,r.kt)("li",{parentName:"ul"},"Contract: ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"After Selecting columns by SQL, you need to reselect the desired columns with the help of an internal source. Following transformation arguments can be passed:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"outputColumnName"),": The column where the final feature will be written and ",(0,r.kt)("inlineCode",{parentName:"li"},"FeatureRow")," are synonyms with ",(0,r.kt)("inlineCode",{parentName:"li"},"FeaturesWithType")," UDF and a single feature is represented by an element in an array."))))),(0,r.kt)("li",{parentName:"ul"},"Functionality:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Converts to feast Features from post processors. This is required to do aggregation and feature transformation from a single dagger."),(0,r.kt)("li",{parentName:"ul"},"Can be used only on ",(0,r.kt)("inlineCode",{parentName:"li"},"post-processor")))),(0,r.kt)("li",{parentName:"ul"},"Example:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"SQL:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},"SELECT\n  data1,\n  data2\nFROM\n  data_stream\n"))),(0,r.kt)("li",{parentName:"ul"},"POST PROCESSOR CONFIG:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},'{\n  "internal_source": [\n    {\n      "output_field": "features",\n      "value": "test",\n      "type": "constant"\n    },\n    {\n      "output_field": "data1",\n      "value": "data1",\n      "type": "sql"\n    },\n    {\n      "output_field": "data2",\n      "value": "data2",\n      "type": "sql"\n    }\n  ],\n  "transformers": [\n    {\n      "transformation_class": "io.odpf.dagger.functions.transformers.FeatureTransformer",\n      "transformation_arguments": {\n        "outputColumnName": "features",\n        "data": [\n          {\n            "keyColumnName": "data1",\n            "valueColumnName": "data2",\n            "type": "StringType"\n          }\n        ]\n      }\n    }\n  ]\n}\n')))))),(0,r.kt)("h3",{id:"hashtransformer"},"HashTransformer"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Transformation Class:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"io.odpf.dagger.functions.transformers.HashTransformer")))),(0,r.kt)("li",{parentName:"ul"},"Contract: ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"After Selecting columns by SQL, you need to reselect the desired columns with the help of an internal source. Following transformation arguments can be passed:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"maskColumns"),": A list of fields that need to be encrypted/masked."))))),(0,r.kt)("li",{parentName:"ul"},"Functionality:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Enables encryption on a set of fields as configured. Used in Data forwarding daggers to clone production data to integration environments with encryption on sensitive data fields. We are using SHA-256 hashing to encrypt data."),(0,r.kt)("li",{parentName:"ul"},"Can be used only on ",(0,r.kt)("inlineCode",{parentName:"li"},"post-processor")))),(0,r.kt)("li",{parentName:"ul"},"Limitations:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Currently support masking on Non-Complex Fields of Data type Integer, Big Integer, and String. However, you can encrypt nested fields of complex data using ",(0,r.kt)("inlineCode",{parentName:"li"},".")," notations. For example test_data.customer_id is a valid argument which will encrypt the customer_id inside test_data. "),(0,r.kt)("li",{parentName:"ul"},"All other Data types including Arrays, complex fields, and other primitive types like boolean are not supported."))),(0,r.kt)("li",{parentName:"ul"},"Example:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"SQL:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},"SELECT\n  event_timestamp,\n  test_data\nFROM\n  data_stream\n"))),(0,r.kt)("li",{parentName:"ul"},"POST PROCESSOR CONFIG:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},'{\n  "internal_source": [\n    {\n      "output_field": "*",\n      "value": "*",\n      "type": "sql"\n    }\n  ],\n  "transformers": [\n    {\n      "transformation_class": "io.odpf.dagger.functions.transformers.HashTransformer",\n      "transformation_arguments": {\n        "maskColumns": [\n          "test_data.data1"\n        ]\n      }\n    }\n  ]\n}\n')))))),(0,r.kt)("h3",{id:"invalidrecordfiltertransformer"},"InvalidRecordFilterTransformer"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Transformation Class:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"io.odpf.dagger.functions.transformers.InvalidRecordFilterTransformer")))),(0,r.kt)("li",{parentName:"ul"},"Contract: ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Following transformation arguments can be passed:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"transformation_arguments"),": A key-value map required for parameters required for the custom transformation class."))))),(0,r.kt)("li",{parentName:"ul"},"Functionality:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Filter the invalid records produced by dagger."),(0,r.kt)("li",{parentName:"ul"},"Can be used only on ",(0,r.kt)("inlineCode",{parentName:"li"},"pre-processor")))),(0,r.kt)("li",{parentName:"ul"},"Example:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"SQL:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},"SELECT \n  data1, \n  data2, \n  event_timestamp \nFROM \n  data_stream\n"))),(0,r.kt)("li",{parentName:"ul"},"PRE PROCESSOR CONFIG:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},'{\n  "table_transformers": [\n    {\n      "table_name": "testtable",\n      "transformers": [\n        {\n          "transformation_class": "io.odpf.dagger.functions.transformers.InvalidRecordFilterTransformer",\n          "transformation_arguments": "testtable"\n        }\n      ]\n    }\n  ]\n}\n')))))),(0,r.kt)("h3",{id:"sqltransformer"},"SQLTransformer"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Transformation Class:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"io.odpf.dagger.functions.transformers.SQLTransformer")))),(0,r.kt)("li",{parentName:"ul"},"Contract: ",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"After Selecting columns by SQL, you need to reselect the desired columns with the help of an internal source. Following transformation arguments can be passed:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"sqlQuery"),": The SQL query for transformation"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"tableName"),"(optional): The table name to be used in the above SQL(default: data_stream)"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"allowedLatenessInMs"),"(optional): The allowed lateness for the events streaming in Kafka(default: 0))"))))),(0,r.kt)("li",{parentName:"ul"},"Functionality:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Enables applying a SQL transformation on top of streaming data in post processors. Primarily useful if users want to apply SQL transformation/aggregation using fields added via External/Internal Post Processors."),(0,r.kt)("li",{parentName:"ul"},"Can be used only on ",(0,r.kt)("inlineCode",{parentName:"li"},"post-processor")))),(0,r.kt)("li",{parentName:"ul"},"Example:",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"SQL:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},"SELECT\n  data1,\n  data2,\n  rowtime\nFROM\n  data_stream\n"))),(0,r.kt)("li",{parentName:"ul"},"POST PROCESSOR CONFIG:",(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},'{\n  "internal_source": [\n    {\n      "output_field": "data1",\n      "value": "data1",\n      "type": "sql"\n    },\n    {\n      "output_field": "rowtime",\n      "value": "rowtime",\n      "type": "sql"\n    },\n    {\n      "output_field": "data2",\n      "value": "data2",\n      "type": "sql"\n    }\n  ],\n  "transformers": [\n    {\n      "transformation_class": "io.odpf.dagger.functions.transformers.SQLTransformer",\n      "transformation_arguments": {\n        "sqlQuery": "SELECT count(distinct data1) AS `count`, data2, TUMBLE_END(rowtime, INTERVAL \'60\' SECOND) AS event_timestamp FROM data_stream group by TUMBLE (rowtime, INTERVAL \'60\' SECOND), data2"\n      }\n    }\n  ]\n}\n')))))))}u.isMDXComponent=!0}}]);