import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';

const FeatureList = [
  {
    title: 'Reliable & consistent processing',
    description: (
      <>
        Provides built-in support for fault-tolerant execution that is
        consistent and correct regardless of data size, cluster size,
        processing pattern or pipeline complexity.
      </>
    ),
  },
  {
    title: 'Robust recovery mechanism',
    description: (
      <>
        Checkpoints, Savepoints & State-backup ensure that even in
        unforeseen circumstances, clusters & jobs can be brought back within minutes.
      </>
    ),
  },
  {
    title: 'SQL and more',
    description: (
      <>
        Define business logic in a query & kick-start your streaming job;
        but it is not just that, there is support for user-defined functions
        & pre-defined transformations.
      </>
    ),
  },
];

function Feature({ title, description }) {
  return (
    <div className={clsx('col col--4')}>
      <div className="padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
