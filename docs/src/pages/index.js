import React from 'react';
import Layout from '@theme/Layout';
import clsx from 'clsx';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Container from '../core/Container';
import GridBlock from '../core/GridBlock';
import useBaseUrl from '@docusaurus/useBaseUrl';

const Hero = () => {
  const { siteConfig } = useDocusaurusContext();
  return (
    <div className="homeHero">
      <div className="logo"><img src={useBaseUrl('img/pattern.svg')} /></div>
      <div className="container banner">
        <div className="row">
          <div className={clsx('col col--5')}>
            <div className="homeTitle">Stream processing made easy</div>
            <small className="homeSubTitle">Configuration over code, cloud-native framework built on top of Apache Flink for stateful processing of real-time streaming data.</small>
            <a className="button" href="docs/intro">Documentation</a>
          </div>
          <div className={clsx('col col--1')}></div>
          <div className={clsx('col col--6')}>
            <div className="text--right"><img src={useBaseUrl('img/query.svg')} /></div>
          </div>
        </div>
      </div>
    </div >
  );
};

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={`Stream processing made easy`}
      description="Stream processing framework">
      <Hero />
      <main>
        <Container className="textSection wrapper" background="light">
          <h1>Built for scale</h1>
          <p>
            Dagger or Data Aggregator is an easy-to-use, configuration over code, cloud-native
            framework built on top of Apache Flink for stateful processing of real-time streaming data.
            With Dagger, you don't need to write custom applications to process data
            in real-time. Instead, you can write SQLs to do the processing and analysis on streaming data.
          </p>
          <GridBlock
            layout="threeColumn"
            contents={[
              {
                title: 'Reliable & consistent processing',
                content: (
                  <div>
                    Provides built-in support for fault-tolerant execution that is
                    consistent and correct regardless of data size, cluster size,
                    processing pattern or pipeline complexity.
                  </div>
                ),
              },
              {
                title: 'Robust recovery mechanism',
                content: (
                  <div>
                    Checkpoints, Savepoints & State-backup ensure that even in
                    unforeseen circumstances, clusters & jobs can be brought back within minutes.
                  </div>
                ),
              },
              {
                title: 'SQL and more',
                content: (
                  <div>
                    Define business logic in a query & kick-start your streaming job;
                    but it is not just that, there is support for user-defined functions
                    & pre-defined transformations.
                  </div>
                ),
              },
              {
                title: 'Scale',
                content: (
                  <div>
                    Dagger scales in an instant, both vertically and horizontally for high
                    performance streaming sink and zero data drops.
                  </div>
                ),
              },
              {
                title: 'Extensibility',
                content: (
                  <div>
                    Add your own sink to dagger with a clearly defined interface or
                    choose from already provided ones.
                  </div>
                ),
              },
              {
                title: 'Flexibility',
                content: (
                  <div>
                    Add custom business logic in form of plugins (UDFs, Transformers,
                    Preprocessors and Post Processors) independent of the core logic.
                  </div>
                ),
              },
            ]}
          />
        </Container>
        <Container className="textSection wrapper" background="dark">
          <h1>Key Features</h1>
          <p>
            Stream processing platform for transforming, aggregating and enriching
            data in real-time mode with ease of operation & unbelievable reliability.
            Dagger can deployd in VMs or cloud-native environment to makes resource provisioning and deployment
            simple & straight-forward, the only limit to your data processing is your imagination.
          </p>
          <GridBlock
            contents={[
              {
                title: 'Aggregations',
                content: (
                  <div>
                    Supports Tumble & Slide for time-windows. Longbow feature
                    supports large windows upto 30-day.
                  </div>
                ),
              },
              {
                title: 'SQL Support',
                content: (
                  <div>
                    Query writing made easy through formatting, suggestions,
                    auto-completes and template queries.
                  </div>
                ),
              },
              {
                title: 'Stream Enrichment',
                content: (
                  <div>
                    Enrich Kafka messages from HTTP endpoints or database sources to bring
                    offline & reference data context to real-time processing.
                  </div>
                ),
              },
              {
                title: 'Observability',
                content: (
                  <div>
                    Always know what’s going on with your deployment with built-in monitoring
                    of throughput, response times, errors and more.
                  </div>
                ),
              },
              {
                title: 'Analytics Ecosystem',
                content: (
                  <div>
                    Dagger can transform, aggregate, join and enrich data in real-time
                    for operational analytics using InfluxDB, Grafana and others.
                  </div>
                ),
              },
              {
                title: 'Stream Transformations',
                content: (
                  <div>
                    Convert Kafka messages on the fly for a variety of
                    use-cases such as feature engineering.
                  </div>
                ),
              },
            ]}
          />
        </Container>
        <Container className="textSection wrapper" background="light">
          <h1>Proud Users</h1>
          <p>
            Dagger was originally created for the Gojek data processing platform,
            and it has been used, adapted and improved by other teams internally and externally.
          </p>
          <GridBlock className="logos"
            layout="fourColumn"
            contents={[
              {
                content: (
                  <img src={useBaseUrl('users/gojek.png')} />
                ),
              },
              {
                content: (
                  <img src={useBaseUrl('users/midtrans.png')} />
                ),
              },
              {
                content: (
                  <img src={useBaseUrl('users/mapan.png')} />
                ),
              },
              {
                content: (
                  <img src={useBaseUrl('users/moka.png')} />
                ),
              },
              // {
              //   content: (
              //     <img src={useBaseUrl('users/goto.png')} />
              //   ),
              // },
              // {
              //   content: (
              //     <img src={useBaseUrl('users/jago.png')} />
              //   ),
              // }
            ]}>
          </GridBlock>
        </Container>
      </main>
    </Layout >
  );
}
