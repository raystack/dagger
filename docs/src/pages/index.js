import React from 'react';
import Layout from '@theme/Layout';
import clsx from 'clsx';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import HomepageFeatures from '../components/HomepageFeatures';
import useBaseUrl from '@docusaurus/useBaseUrl';


const HomeSplash = () => {
  const { siteConfig } = useDocusaurusContext();
  return (
    <div>
      <div className="homeContainer">
        <div className="homeSplashFade">
          <div className="logo">
            <img src={useBaseUrl('img/pattern.svg')} />
          </div>
          <div className="wrapper homeWrapper">
            <h2 className="projectTitle">
              {siteConfig.title}
              <small>{siteConfig.tagline}</small>
              <small>{siteConfig.subtagline}</small>
            </h2>
            <div className="try-it">
              <h3>
                <a className="button" href="/docs/intro">
                  Read the Docs
                </a>
              </h3>
            </div>
          </div>
        </div>
      </div>
    </div >
  );
};

const Hero = () => {
  const { siteConfig } = useDocusaurusContext();
  return (
    <div className="homeHero">
      <div className="container banner">
        <div className="row">
          <div className={clsx('col col--5')}>
            <div className="xTitle">Stream processing made easy</div>
            <small className="xSubTitle">Configuration over code, cloud-native framework built on top of Apache Flink for stateful processing of real-time streaming data.</small>
            <a className="" href="/docs/intro">Read the docs</a>
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
      title={`Hello from ${siteConfig.title}`}
      description="Stream processing framework">
      <Hero />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
