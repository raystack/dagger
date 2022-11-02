"use strict";(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[6905],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>f});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var d=a.createContext({}),s=function(e){var t=a.useContext(d),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=s(e.components);return a.createElement(d.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},c=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,d=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),c=s(n),f=r,h=c["".concat(d,".").concat(f)]||c[f]||u[f]||i;return n?a.createElement(h,o(o({ref:t},p),{},{components:n})):a.createElement(h,o({ref:t},p))}));function f(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=c;var l={};for(var d in t)hasOwnProperty.call(t,d)&&(l[d]=t[d]);l.originalType=e,l.mdxType="string"==typeof e?e:r,o[1]=l;for(var s=2;s<i;s++)o[s]=n[s];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}c.displayName="MDXCreateElement"},4585:(e,t,n)=>{n.r(t),n.d(t,{contentTitle:()=>o,default:()=>p,frontMatter:()=>i,metadata:()=>l,toc:()=>d});var a=n(7462),r=(n(7294),n(3905));const i={},o=void 0,l={unversionedId:"rfcs/python_udf",id:"rfcs/python_udf",isDocsHomePage:!1,title:"python_udf",description:"Motivation",source:"@site/docs/rfcs/20220504_python_udf.md",sourceDirName:"rfcs",slug:"/rfcs/python_udf",permalink:"/dagger/docs/rfcs/python_udf",editUrl:"https://github.com/odpf/dagger/edit/master/docs/docs/rfcs/20220504_python_udf.md",tags:[],version:"current",sidebarPosition:20220504,frontMatter:{}},d=[{value:"Motivation",id:"motivation",children:[]},{value:"Requirement",id:"requirement",children:[]},{value:"Python User Defined Function",id:"python-user-defined-function",children:[]},{value:"Configuration",id:"configuration",children:[]},{value:"Registering the Udf",id:"registering-the-udf",children:[]},{value:"Release the Udf",id:"release-the-udf",children:[]},{value:"Reference",id:"reference",children:[]}],s={toc:d};function p(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},s,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h2",{id:"motivation"},"Motivation"),(0,r.kt)("p",null,"Dagger users include developers, analysts, data scientists, etc. For users to use Dagger, they can add new capabilities by defining their own functions commonly referred to as UDFs. Currently, Dagger only supports java as the language for the UDFs. To democratize the process of creating and maintaining the UDFs we want to add support for python."),(0,r.kt)("h2",{id:"requirement"},"Requirement"),(0,r.kt)("p",null,"Support for adding Python UDF on Dagger\nEnd-to-end flow on adding and using Python UDF on Dagger. "),(0,r.kt)("h2",{id:"python-user-defined-function"},"Python User Defined Function"),(0,r.kt)("p",null,"There are two kinds of Python UDF that can be registered on Dagger:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"General Python UDF"),(0,r.kt)("li",{parentName:"ul"},"Vectorized Python UDF")),(0,r.kt)("p",null,'It shares a similar way as the general user-defined functions on how to define vectorized user-defined functions. Users only need to add an extra parameter func_type="pandas" in the decorator udf or udaf to mark it as a vectorized user-defined function.'),(0,r.kt)("table",null,(0,r.kt)("thead",{parentName:"table"},(0,r.kt)("tr",{parentName:"thead"},(0,r.kt)("th",{parentName:"tr",align:null},"Type"),(0,r.kt)("th",{parentName:"tr",align:null},"General Python UDF"),(0,r.kt)("th",{parentName:"tr",align:null},"Vectorized Python UDF"))),(0,r.kt)("tbody",{parentName:"table"},(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Data Processing Method"),(0,r.kt)("td",{parentName:"tr",align:null},"One piece of data is processed each time a UDF is called"),(0,r.kt)("td",{parentName:"tr",align:null},"Multiple pieces of data are processed each time a UDF is called")),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Serialization/Deserialization"),(0,r.kt)("td",{parentName:"tr",align:null},"Serialization and Deserialization are required for each piece of data on the Java side and Python side"),(0,r.kt)("td",{parentName:"tr",align:null},"The data transmission format between Java and Python is based on Apache Arrow: ",(0,r.kt)("ul",null,(0,r.kt)("li",null," Pandas supports Apache Arrow natively, so serialization and deserialization are not required on Python side"),(0,r.kt)("li",null,"On the Java side, vectorized optimization is possible, and serialization/deserialization can be avoided as much as possible")))),(0,r.kt)("tr",{parentName:"tbody"},(0,r.kt)("td",{parentName:"tr",align:null},"Exection Performance"),(0,r.kt)("td",{parentName:"tr",align:null},"Poor"),(0,r.kt)("td",{parentName:"tr",align:null},"Good",(0,r.kt)("ul",null,(0,r.kt)("li",null,"Vectorized execution is of high efficiency"),(0,r.kt)("li",null,"High-performance python UDF can be implemented based on high performance libraries such as pandas and numpy")))))),(0,r.kt)("p",null,"Note: "),(0,r.kt)("p",null,"When using vectorized udf, Flink will convert the messages to pandas.series, and the udf will use that as an input and the output also pandas.series. The pandas.series size for input and output should be the same."),(0,r.kt)("h2",{id:"configuration"},"Configuration"),(0,r.kt)("p",null,"There are a few configurations that required for using python UDF, and also options we can adjust for optimization."),(0,r.kt)("p",null,'Configuration that will be added on Dagger codebase:\n| Key | Default | Type | Example\n| --- | ---     | ---  | ----  |\n|PYTHON_UDF_ENABLE|false|Boolean|false|\n|PYTHON_UDF_CONFIG|(none)|String|{"PYTHON_FILES":"/path/to/files.zip", "PYTHON_REQUIREMENTS": "requirements.txt", "PYTHON_FN_EXECUTION_BUNDLE_SIZE": "1000"}|'),(0,r.kt)("p",null,"The following variables than can be configurable on ",(0,r.kt)("inlineCode",{parentName:"p"},"PYTHON_UDF_CONFIG"),":\n| Key | Default | Type | Example\n| --- | ---     | ---  | ----  |\n|PYTHON_ARCHIVES|(none)|String|/path/to/data.zip|\n|PYTHON_FILES|(none)|String|/path/to/files.zip|\n|PYTHON_REQUIREMENTS|(none)|String|/path/to/requirements.txt|\n|PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE|10000|Integer|10000|\n|PYTHON_FN_EXECUTION_BUNDLE_SIZE|100000|Integer|100000|\n|PYTHON_FN_EXECUTION_BUNDLE_TIME|1000|Long|1000|"),(0,r.kt)("h2",{id:"registering-the-udf"},"Registering the Udf"),(0,r.kt)("p",null,"Dagger will automatically register the python udf as long as the files meets the following criteria:"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Python file names should be the same with its function method\nExample:"),(0,r.kt)("p",{parentName:"li"},"sample.py"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre"},'from pyflink.table import DataTypes\nfrom pyflink.table.udf import udf\n\n\n@udf(result_type=DataTypes.STRING())\ndef sample(word: str):\n    return word + "_test"\n'))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"Avoid adding duplicate ",(0,r.kt)("inlineCode",{parentName:"p"},".py")," filenames. e.g: ",(0,r.kt)("inlineCode",{parentName:"p"},"__init__.py")))),(0,r.kt)("h2",{id:"release-the-udf"},"Release the Udf"),(0,r.kt)("p",null,"List of udfs for dagger, will be added on directory ",(0,r.kt)("inlineCode",{parentName:"p"},"dagger-py-functions")," include with its test, data files that are used on the udf, and the udf dependency(requirements.txt).\nAll of these files will be bundled to single zip file and uploaded to assets on release."),(0,r.kt)("h2",{id:"reference"},"Reference"),(0,r.kt)("p",null,(0,r.kt)("a",{parentName:"p",href:"https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/table/udfs/overview/"},"Flink Python User Defined Functions")))}p.isMDXComponent=!0}}]);