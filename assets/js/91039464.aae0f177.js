"use strict";(self.webpackChunkdagger=self.webpackChunkdagger||[]).push([[7356],{3905:(e,t,a)=>{a.d(t,{Zo:()=>c,kt:()=>g});var r=a(7294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},o=Object.keys(e);for(r=0;r<o.length;r++)a=o[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)a=o[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var l=r.createContext({}),p=function(e){var t=r.useContext(l),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},c=function(e){var t=p(e.components);return r.createElement(l.Provider,{value:t},e.children)},m="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},u=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,o=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),m=p(a),u=n,g=m["".concat(l,".").concat(u)]||m[u]||d[u]||o;return a?r.createElement(g,i(i({ref:t},c),{},{components:a})):r.createElement(g,i({ref:t},c))}));function g(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=a.length,i=new Array(o);i[0]=u;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[m]="string"==typeof e?e:n,i[1]=s;for(var p=2;p<o;p++)i[p]=a[p];return r.createElement.apply(null,i)}return r.createElement.apply(null,a)}u.displayName="MDXCreateElement"},2157:(e,t,a)=>{a.r(t),a.d(t,{contentTitle:()=>i,default:()=>m,frontMatter:()=>o,metadata:()=>s,toc:()=>l});var r=a(7462),n=(a(7294),a(3905));const o={},i="Dagger Quickstart",s={unversionedId:"guides/quickstart",id:"guides/quickstart",isDocsHomePage:!1,title:"Dagger Quickstart",description:"There are 2 ways to set up and get dagger running in your machine in no time -",source:"@site/docs/guides/quickstart.md",sourceDirName:"guides",slug:"/guides/quickstart",permalink:"/dagger/docs/guides/quickstart",editUrl:"https://github.com/raystack/dagger/edit/master/docs/docs/guides/quickstart.md",tags:[],version:"current",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Overview",permalink:"/dagger/docs/guides/overview"},next:{title:"Choosing a Source",permalink:"/dagger/docs/guides/choose_source"}},l=[{value:"Docker Compose Setup",id:"docker-compose-setup",children:[{value:"Prerequisites",id:"prerequisites",children:[]},{value:"Workflow",id:"workflow",children:[]}]},{value:"Local Installation Setup",id:"local-installation-setup",children:[{value:"Prerequisites",id:"prerequisites-1",children:[]},{value:"Quickstart",id:"quickstart",children:[]},{value:"Troubleshooting",id:"troubleshooting",children:[]}]}],p={toc:l},c="wrapper";function m(e){let{components:t,...a}=e;return(0,n.kt)(c,(0,r.Z)({},p,a,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"dagger-quickstart"},"Dagger Quickstart"),(0,n.kt)("p",null,"There are 2 ways to set up and get dagger running in your machine in no time -"),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("strong",{parentName:"li"},(0,n.kt)("a",{parentName:"strong",href:"/dagger/docs/guides/quickstart#docker-compose-setup"},"Docker Compose Setup"))," - recommended for beginners"),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("strong",{parentName:"li"},(0,n.kt)("a",{parentName:"strong",href:"/dagger/docs/guides/quickstart#local-installation-setup"},"Local Installation Setup"))," - for more advanced usecases")),(0,n.kt)("h2",{id:"docker-compose-setup"},"Docker Compose Setup"),(0,n.kt)("h3",{id:"prerequisites"},"Prerequisites"),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("strong",{parentName:"li"},"You must have docker installed"))),(0,n.kt)("p",null,"Following are the steps for setting up dagger in docker compose -"),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Clone Dagger repository into your local"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"git clone https://github.com/raystack/dagger.git\n"))),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"cd into the docker-compose directory:"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"cd dagger/quickstart/docker-compose\n"))),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"fire this command to spin up the docker compose:"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"docker compose up\n")),(0,n.kt)("p",{parentName:"li"},"This will spin up docker containers for the kafka, zookeeper, stencil, kafka-producer and the dagger.")),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"fire this command to gracefully stop all the docker containers. This will save the container state and help to speed up the setup next time. All the kafka records and topics will also be saved :"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"docker compose stop\n")),(0,n.kt)("p",{parentName:"li"},"To start the containers from their saved state run this command"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"docker compose start\n"))),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"fire this command to gracefully remove all the containers. This will delete all the kafka topics/ saved data as well:"),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"docker compose down\n")))),(0,n.kt)("h3",{id:"workflow"},"Workflow"),(0,n.kt)("p",null,"Following are the containers that are created, in chronological order, when you run ",(0,n.kt)("inlineCode",{parentName:"p"},"docker compose up")," -"),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("strong",{parentName:"li"},"Zookeeper")," - Container for the Zookeeper service is created and listening on port 2187. Zookeeper is a service required by the Kafka server."),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("strong",{parentName:"li"},"Kafka")," - Container for Kafka server is created and is exposed on port 29094. This will serve as the input data source for the Dagger."),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("strong",{parentName:"li"},"init-kafka")," - This container creates the kafka topic ",(0,n.kt)("inlineCode",{parentName:"li"},"dagger-test-topic-v1")," from which the dagger will pull the Kafka messages."),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("strong",{parentName:"li"},"Stencil")," - It compiles the proto file and creates a proto descriptor. Also it sets up an http server serving the proto descriptors required by dagger to parse the Kafka messages."),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("strong",{parentName:"li"},"kafka-producer")," - It runs a script to generate the random kafka messages and sends one message to the kafka topic every second."),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("strong",{parentName:"li"},"Dagger")," - Clones the Dagger Github repository and builds the jar. Then it creates an in-memory flink cluster and uploads the dagger job jar and starts the job.")),(0,n.kt)("p",null,"The dagger environment variables are present in the ",(0,n.kt)("inlineCode",{parentName:"p"},"local.properties")," file inside the ",(0,n.kt)("inlineCode",{parentName:"p"},"quickstart/docker-compose/resources")," directory. The dagger runs a simple aggregation query which will count the number of bookings , i.e. kafka messages, in every 30 seconds interval. The output will be visible in the logs in the terminal itself. You can edit this query (",(0,n.kt)("inlineCode",{parentName:"p"},"FLINK_SQL_QUERY")," variable) in the ",(0,n.kt)("inlineCode",{parentName:"p"},"local.properties")," file inside the ",(0,n.kt)("inlineCode",{parentName:"p"},"quickstart/docker-compose/resources")," directory."),(0,n.kt)("h2",{id:"local-installation-setup"},"Local Installation Setup"),(0,n.kt)("h3",{id:"prerequisites-1"},"Prerequisites"),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("strong",{parentName:"li"},"Your Java version is Java 8"),": Dagger as of now works only with Java 8. Some features might not work with older or later versions."),(0,n.kt)("li",{parentName:"ol"},"Your ",(0,n.kt)("strong",{parentName:"li"},"Kafka")," version is ",(0,n.kt)("strong",{parentName:"li"},"3.0.0")," or a minor version of it"),(0,n.kt)("li",{parentName:"ol"},"You have ",(0,n.kt)("strong",{parentName:"li"},"kcat")," installed: We will use kcat to push messages to Kafka from the CLI. You can follow the installation steps ",(0,n.kt)("a",{parentName:"li",href:"https://github.com/edenhill/kcat"},"here"),". Ensure the version you install is 1.7.0 or a minor version of it."),(0,n.kt)("li",{parentName:"ol"},"You have ",(0,n.kt)("strong",{parentName:"li"},"protobuf")," installed: We will use protobuf to push messages encoded in protobuf format to Kafka topic. You can follow the installation steps for MacOS ",(0,n.kt)("a",{parentName:"li",href:"https://formulae.brew.sh/formula/protobuf"},"here"),". For other OS, please download the corresponding release from ",(0,n.kt)("a",{parentName:"li",href:"https://github.com/protocolbuffers/protobuf/releases"},"here"),". Please note, this quickstart has been written to work with",(0,n.kt)("a",{parentName:"li",href:"https://github.com/protocolbuffers/protobuf/releases/tag/v3.17.3"}," 3.17.3")," of protobuf. Compatibility with other versions is unknown."),(0,n.kt)("li",{parentName:"ol"},"You have ",(0,n.kt)("strong",{parentName:"li"},"Python 2.7+")," and ",(0,n.kt)("strong",{parentName:"li"},"simple-http-server")," installed: We will use Python along with simple-http-server to spin up a mock Stencil server which can serve the proto descriptors to Dagger. To install ",(0,n.kt)("strong",{parentName:"li"},"simple-http-server"),", please follow these ",(0,n.kt)("a",{parentName:"li",href:"https://pypi.org/project/simple-http-server/"},"installation steps"),".")),(0,n.kt)("h3",{id:"quickstart"},"Quickstart"),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},"Clone Dagger repository into your local")),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"git clone https://github.com/raystack/dagger.git\n")),(0,n.kt)("ol",{start:2},(0,n.kt)("li",{parentName:"ol"},"Next, we will generate our proto descriptor set. Ensure you are at the top level directory(",(0,n.kt)("inlineCode",{parentName:"li"},"dagger"),") and then fire this command")),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre"},"./gradlew clean dagger-common:generateTestProto\n")),(0,n.kt)("p",null,"This command will generate a descriptor set containing the proto descriptors of all the proto files present under ",(0,n.kt)("inlineCode",{parentName:"p"},"dagger-common/src/test/proto"),". After running this, you should see a binary file called ",(0,n.kt)("inlineCode",{parentName:"p"},"dagger-descriptors.bin")," under ",(0,n.kt)("inlineCode",{parentName:"p"},"dagger-common/src/generated-sources/descriptors/"),"."),(0,n.kt)("ol",{start:3},(0,n.kt)("li",{parentName:"ol"},"Next, we will setup a mock Stencil server to serve this proto descriptor set to Dagger. Open up a new tab in your terminal and ",(0,n.kt)("inlineCode",{parentName:"li"},"cd")," into this directory: ",(0,n.kt)("inlineCode",{parentName:"li"},"dagger-common/src/generated-sources/descriptors"),". Then fire this command:")),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},"python -m SimpleHTTPServer 8000\n")),(0,n.kt)("p",null,"This will spin up a mock HTTP server and serve the descriptor set we just generated in the previous step at port 8000.\nThe Stencil client being used in Dagger will fetch it by calling this URL. This has been already configured in ",(0,n.kt)("inlineCode",{parentName:"p"},"local.properties"),", as we have set ",(0,n.kt)("inlineCode",{parentName:"p"},"SCHEMA_REGISTRY_STENCIL_ENABLE")," to true and pointed ",(0,n.kt)("inlineCode",{parentName:"p"},"SCHEMA_REGISTRY_STENCIL_URLS")," to ",(0,n.kt)("inlineCode",{parentName:"p"},"http://127.0.0.1:8000/dagger-descriptors.bin"),"."),(0,n.kt)("ol",{start:4},(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},"Next, we will generate and send some messages to a sample kafka topic as per some proto schema. Note that, in ",(0,n.kt)("inlineCode",{parentName:"p"},"local.properties")," we have set ",(0,n.kt)("inlineCode",{parentName:"p"},"INPUT_SCHEMA_PROTO_CLASS")," under ",(0,n.kt)("inlineCode",{parentName:"p"},"STREAMS")," to use ",(0,n.kt)("inlineCode",{parentName:"p"},"org.raystack.dagger.consumer.TestPrimitiveMessage")," proto. Hence, we will push messages which conform to this schema into the topic. For doing this, please follow these steps:"),(0,n.kt)("ol",{parentName:"li"},(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("inlineCode",{parentName:"li"},"cd")," into the directory ",(0,n.kt)("inlineCode",{parentName:"li"},"dagger-common/src/test/proto"),". You should see a text file ",(0,n.kt)("inlineCode",{parentName:"li"},"sample_message.txt")," which contains just one message. We will encode it into a binary in protobuf format."),(0,n.kt)("li",{parentName:"ol"},"Fire this command:")),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-protobuf"},"protoc --proto_path=./ --encode=org.raystack.dagger.consumer.TestPrimitiveMessage ./TestLogMessage.proto < ./sample_message.txt > out.bin\n")),(0,n.kt)("p",{parentName:"li"},"This will generate a binary file called ",(0,n.kt)("inlineCode",{parentName:"p"},"out.bin"),". It contains the binary encoded message of ",(0,n.kt)("inlineCode",{parentName:"p"},"sample_message.txt"),"."),(0,n.kt)("ol",{parentName:"li",start:3},(0,n.kt)("li",{parentName:"ol"},"Next, we will push this encoded message to the source Kafka topic as mentioned under ",(0,n.kt)("inlineCode",{parentName:"li"},"SOURCE_KAFKA_TOPIC_NAMES")," inside ",(0,n.kt)("inlineCode",{parentName:"li"},"STREAMS")," inside ",(0,n.kt)("inlineCode",{parentName:"li"},"local.properties"),". Ensure Kafka is running at ",(0,n.kt)("inlineCode",{parentName:"li"},"localhost:9092")," and then, fire this command:")),(0,n.kt)("pre",{parentName:"li"},(0,n.kt)("code",{parentName:"pre",className:"language-shell"},'kcat -P -b localhost:9092 -D "\\n" -T -t dagger-test-topic-v1 out.bin\n')),(0,n.kt)("p",{parentName:"li"},"You can also fire this command multiple times, if you want multiple messages to be sent into the topic. Just make sure you increment the ",(0,n.kt)("inlineCode",{parentName:"p"},"event_timestamp")," value every time inside ",(0,n.kt)("inlineCode",{parentName:"p"},"sample_message.txt")," and then repeat the above steps.")),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("inlineCode",{parentName:"p"},"cd")," into the repository root again (",(0,n.kt)("inlineCode",{parentName:"p"},"dagger"),") and start Dagger by running the following command:"))),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-shell"},"./gradlew dagger-core:runFlink\n")),(0,n.kt)("p",null,"After some initialization logs, you should see the output of the SQL query getting printed."),(0,n.kt)("h3",{id:"troubleshooting"},"Troubleshooting"),(0,n.kt)("ol",null,(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("strong",{parentName:"p"},"I am pushing messages to the kafka topic but not seeing any output in the logs.")),(0,n.kt)("p",{parentName:"li"},"This can happen for the following reasons:"),(0,n.kt)("p",{parentName:"li"},"a. Pushed messages are not reaching the right topic: Check for any exceptions or errors when pushing messages to the Kafka topic. Ensure that the topic to which you are pushing messages is the same one for which you have configured Dagger to read from under ",(0,n.kt)("inlineCode",{parentName:"p"},"STREAMS")," -> ",(0,n.kt)("inlineCode",{parentName:"p"},"SOURCE_KAFKA_TOPIC_NAMES")," in ",(0,n.kt)("inlineCode",{parentName:"p"},"local.properties")),(0,n.kt)("p",{parentName:"li"},"b. The consumer group is not updated: Dagger might have already processed those messages. If you have made any changes to the setup, make sure you update the ",(0,n.kt)("inlineCode",{parentName:"p"},"STREAMS")," -> ",(0,n.kt)("inlineCode",{parentName:"p"},"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID")," variable in ",(0,n.kt)("inlineCode",{parentName:"p"},"local.properties")," to some new value.")),(0,n.kt)("li",{parentName:"ol"},(0,n.kt)("p",{parentName:"li"},(0,n.kt)("strong",{parentName:"p"},"I see an exception ",(0,n.kt)("inlineCode",{parentName:"strong"},"java.lang.RuntimeException: Unable to retrieve any partitions with KafkaTopicsDescriptor: Topic Regex Pattern"))),(0,n.kt)("p",{parentName:"li"},"This can happen if the topic configured under ",(0,n.kt)("inlineCode",{parentName:"p"},"STREAMS")," -> ",(0,n.kt)("inlineCode",{parentName:"p"},"SOURCE_KAFKA_TOPIC_NAMES")," in ",(0,n.kt)("inlineCode",{parentName:"p"},"local.properties")," is new and you have not pushed any messages to it yet. Ensure that you have pushed atleast one message to the topic before you start dagger."))))}m.isMDXComponent=!0}}]);