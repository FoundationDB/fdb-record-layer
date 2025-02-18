# Contributing to FoundationDB Record Layer

Welcome to the FoundationDB Record Layer community, and thank you for contributing! 
The following guide outlines the basics of how to get involved.

#### Table of Contents

* [Before You Get Started](#before-you-get-started)
  * [Community Guidelines](#community-guidelines)
  * [Project Licensing](#project-licensing)
  * [Governance and Decision Making](#governance-and-decision-making)
* [Contributing](#contributing)
  * [Issues to Track Changes](#issues-to-track-changes)
  * [Opening a Pull Request](#opening-a-pull-request)
  * [Reporting Issues](#reporting-issues)
* [Project Communication](#project-communication)
  * [Community Forums](#community-forums)
  * [Using GitHub Issues and Community Forums](#using-github-issues-and-community-forums)
  * [Project and Development Updates](#project-and-development-updates)

## Before You Get Started

### Community Guidelines

We want the FoundationDB Record Layer community to be as welcoming and inclusive as 
possible, and have adopted a [Code of Conduct](CODE_OF_CONDUCT.md) that we ask all 
community members to read and observe.

### Project Licensing

By submitting a pull request, you represent that you have the right to license your 
contribution to Apple and the community, and agree by submitting the patch that 
your contributions are licensed under the Apache 2.0 license.

### Governance and Decision Making

At project launch, FoundationDB has a light governance structure. The intention is 
for the community to evolve quickly and adopt additional processes as participation 
grows. Stay tuned, and stay engaged! Your feedback is welcome.

We draw inspiration from the Apache Software Foundation's informal motto: 
["community over code"](https://blogs.apache.org/foundation/entry/asf_15_community_over_code), 
and their emphasis on meritocratic rules. You'll also observe that some initial 
community structure is [inspired by the Swift community](https://swift.org/community/#community-structure).

Members of the Apple FoundationDB team are part of the initial core committers helping 
review individual contributions; you'll see them commenting on your pull requests. 
Future committers to the open source project, and the process for adding individuals 
in this role will be formalized in the future.

## Contributing

We happily welcome your contributions! Before diving in, please read the following
contribution guidelines.

### Issues to Track Changes

All contributions to the FoundationDB Record Layer should begin life as a GitHub
issue, describing the nature of the issue or enhancement that you wish to address.
For larger changes, the issue system can also act as an area of tracking the feature's 
design and any sub-issues that need to be implemented as part of your change. 

It is always acceptable and encouraged, of course, to discuss enhancements
and issues in the FoundationDB forums, even prior to opening an issue. But the opening 
of a GitHub issue effectively notifies the community of your work (or intent to work).
And, more importantly, by letting the community know that you intend to get
involved before you begin any significant development effort avoids multiple
people solving the same problem, or wasted work in developing a solution that 
may not align with the design or goals of the project.  Finally, and most 
importantly, it keeps the community open and communicative.

### Opening a Pull Request

When opening a pull request (PR), please ensure that the title of the pull request
or the commit message indicates the issue that it is addressing (see 
[Closing issues with keywords](https://help.github.com/articles/closing-issues-using-keywords/)).
For example:

    Fixes #492: Remove excessive frobnification in online index rebuild

This will automatically create an association between the PR and the issue that
it is addressing and, upon merging of the PR into the main code line, will 
automatically mark the issue as resolved.

If your pull request results in a user-visible change to the Record Layer, you should
also update the [release notes](docs/sphinx/source/ReleaseNotes.md). For most changes, it
is sufficient to fill in one of the bullets in the "next release" section of that
document. You should include a short description of the change as well as filling in
the issue number. The "next release" section is commented out, so the change won't
be visible in our documentation until the next time a release is cut.

### Reporting issues

Please refer to the section below on [using GitHub issues and the community forums](#using-github-issues-and-community-forums) for more info.

#### Security issues

To report a security issue, please **DO NOT** start by filing a public issue 
or posting to the forums; instead send a private email to 
[fdb-oss-security@group.apple.com](mailto:fdb-oss-security@group.apple.com).

## Project Communication

### Community Forums

We encourage your participation asking questions and helping improve the FoundationDB
Record Layer project. Check out the [FoundationDB community forums](https://forums.foundationdb.org).
These forums are shared with the FoundationDB project, and serve a similar function as 
mailing lists in many open source projects. The forums are organized into three sections:

* [FoundationDB Layer Development](https://forums.foundationdb.org/c/development/fdb-layers)
  FoundationDB forum for discussing layer development.
* [FoundationDB Development](https://forums.foundationdb.org/c/development): Used to discuss 
  more general FDB development.
* [Using FoundationDB](https://forums.foundationdb.org/c/using-foundationdb): For 
  discussing user-facing topics. Getting started and have a question? This is 
  the place for you.
* [Site Feedback](https://forums.foundationdb.org/c/site-feedback): A category for 
  discussing the forums and the OSS project, its organization, how it works, and how 
  we can improve it.

### Using GitHub Issues and Community Forums

GitHub issues and the community forums both offer features to facilitate discussion. To 
clarify how and when to use each tool, here's a quick summary of how we think about them:

GitHub Issues should be used for tracking tasks. If you know the specific code that needs 
to be changed, then it should go to GitHub Issues. Everything else should go to the Forums. 
For example: 

* I am experiencing some weird behavior, which I think is a bug, but I don't know where 
  exactly (mysteries and unexpected behaviors): *Forums*
* I see a bug in this piece of code: *GitHub Issues*
* Feature requests and design discussion: *Forums* (output of final design should be 
  included with the resulting GitHub issue)
* Implementing an agreed upon feature: *GitHub Issues*

In particular, proposed major additions to the project should be discussed in the 
forums rather than opening an issue immediately followed by a pull request. 

### Project and Development Updates
Stay connected to the project and the community! For project and community updates, 
follow the [FoundationDB project blog](https://www.foundationdb.org/blog/). Development 
announcements will be made via the community forums' 
[dev-announce](https://forums.foundationdb.org/c/development/dev-announce) section.

