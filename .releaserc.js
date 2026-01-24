module.exports = {
  branches: ['main'],
  plugins: [
    '@semantic-release/commit-analyzer',
    '@semantic-release/release-notes-generator',
    '@semantic-release/changelog',
    [
      '@semantic-release/exec',
      {
        prepareCmd: [
          'yq e -i \'.version = "${nextRelease.version}"\' helm/Chart.yaml',
          'yq e -i \'.appVersion = "${nextRelease.version}"\' helm/Chart.yaml',
          'yq e -i \'.image.tag = "${nextRelease.version}"\' helm/values.yaml',
          'sed -i "s/--version [0-9]\\+\\.[0-9]\\+\\.[0-9]\\+/--version ${nextRelease.version}/g" README.md',
          'sed -i "s/gh release download v[0-9]\\+\\.[0-9]\\+\\.[0-9]\\+/gh release download v${nextRelease.version}/g" README.md',
          'sed -i "s/s3-resource-operator:[0-9]\\+\\.[0-9]\\+\\.[0-9]\\+/s3-resource-operator:${nextRelease.version}/g" README.md',
          'MAJOR=$(echo ${nextRelease.version} | cut -d. -f1) && MINOR=$(echo ${nextRelease.version} | cut -d. -f1-2) && sed -i -E "s/Tags: \`latest\`, \`[0-9]+\\.[0-9]+\\.[0-9]+\`, \`[0-9]+\\.[0-9]+\`, \`[0-9]+\`/Tags: \`latest\`, \`${nextRelease.version}\`, \`$MINOR\`, \`$MAJOR\`/g" README.md'
        ].join(' && '),
      },
    ],
    [
      '@semantic-release/git',
      {
        assets: ['helm/Chart.yaml', 'helm/values.yaml', 'CHANGELOG.md', 'README.md'],
        message: 'chore(release): ${nextRelease.version} [skip ci]\n\n${nextRelease.notes}',
      },
    ],
    '@semantic-release/github',
  ],
};
