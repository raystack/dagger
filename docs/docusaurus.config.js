const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

// With JSDoc @type annotations, IDEs can provide config autocompletion
/** @type {import('@docusaurus/types').DocusaurusConfig} */
(module.exports = {
  title: 'Dagger',
  tagline: 'Stream processing framework',
  url: 'https://odpf.github.io/',
  baseUrl: '/dagger/',
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
          { to: '/help', label: 'Help', position: 'left' },
          {
            href: 'https://bit.ly/2RzPbtn',
            position: 'right',
            className: 'header-slack-link',
          },
          {
            href: 'https://github.com/odpf/dagger',
            className: 'navbar-item-github',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'light',
        links: [
          {
            title: 'Products',
            items: [
              { label: 'Optimus', href: 'https://github.com/odpf/optimus' },
              { label: 'Firehose', href: 'https://github.com/odpf/firehose' },
              { label: 'Raccoon', href: 'https://github.com/odpf/raccoon' },
              { label: 'Stencil', href: 'https://github.com/odpf/stencil' },
            ],
          },
          {
            title: 'Resources',
            items: [
              { label: 'Docs', to: '/docs/intro' },
              { label: 'Blog', to: '/blog', },
              { label: 'Help', to: '/help', },
            ],
          },
          {
            title: 'Community',
            items: [
              { label: 'Slack', href: 'https://bit.ly/2RzPbtn' },
              { label: 'GitHub', href: 'https://github.com/odpf/dagger' }
            ],
          },
        ],
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
      gtag: {
        trackingID: 'G-KDHSNBDXW8',
      },
      announcementBar: {
        id: 'star-repo',
        content: '⭐️ If you like Dagger, give it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/odpf/dagger">GitHub</a>! ⭐',
        backgroundColor: '#222',
        textColor: '#eee',
        isCloseable: true,
      },
    }),
});
