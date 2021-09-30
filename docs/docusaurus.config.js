const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

// With JSDoc @type annotations, IDEs can provide config autocompletion
/** @type {import('@docusaurus/types').DocusaurusConfig} */
(module.exports = {
  title: 'Dagger',
  tagline: 'Stream processing framework',
  url: 'odpf.github.io/',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'ODPF',
  projectName: 'dagger',

  presets: [
    [
      '@docusaurus/preset-classic',
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/odpf/dagger/edit/master/docs/',
        },
        blog: {
          showReadingTime: true,
          editUrl:
            'https://github.com/odpf/optimus/edit/master/docs/blog/',
        },
        theme: {
          customCss: [
            require.resolve('./src/css/theme.css'),
            require.resolve('./src/css/custom.css')
          ],
        },
      }),
    ],
  ],

  themeConfig:
    ({
      colorMode: {
        defaultMode: 'light',
        respectPrefersColorScheme: true,
        switchConfig: {
          darkIcon: '☾',
          lightIcon: '☀️',
        },
      },
      navbar: {
        title: 'Dagger',
        logo: { src: 'img/logo-n.svg', },
        items: [
          {
            type: 'doc',
            docId: 'intro',
            position: 'left',
            label: 'Docs',
          },
          { to: '/blog', label: 'Blog', position: 'left' },
          {
            href: 'https://github.com/odpf/dagger',
            className: 'navbar-item-github',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [{ label: 'Docs', to: '/docs/intro', },],
          },
          {
            title: 'Community',
            items: [{ label: 'Slack', href: 'https://bit.ly/2RzPbtn', }],
          },
          {
            title: 'More',
            items: [
              { label: 'Blog', to: '/blog', },
              { label: 'GitHub', href: 'https://github.com/odpf/dagger', },
            ],
          },
        ],
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
});
