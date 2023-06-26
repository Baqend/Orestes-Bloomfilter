Contributing
============

We are working on a feature branch basis. That means, if you want to impelement a new feature, refactor something, or
add any other change, you branch you feature branch of the master branch. That is the branch on which you will do you work.
The following sections will tell you how you should you feature branches, how to keep your feature branch up to date and
how to write good commit messages.

Feature branch mames
--------------------

Branches should fulfill the following name scheme:

    <year>/<calendarweek>/<kind>/<name_with_underscores>

For example:

    2023/44/feat/disable_speed_kit_on_many_errors
    2023/27/fix/implement_feature_xyz

Keeping your feature branch up to date
--------------------------------------

Make sure to regularily __rebase__ your feature branch onto the master branch to prevent diverging too much from it.
You might miss changes which could touch the files you're working on which will result in a lot of merge conflicts.

The simplest way to do that is by using IntelliJ:
* Check out your feature branch
* Updat the master branch
* Rebase your branch onto the master branch by selecting the master branch and choosing `Rebase current onto selected`

Take a look [here](https://www.jetbrains.com/help/idea/apply-changes-from-one-branch-to-another.html#rebase-branch) for more details on how to properly rebase with IntelliJ.

Commit message format and content
---------------------------------

Commits should have the following scheme:

    <kind>(<scope>): <message>

With the following values for `<kind>`:

- **feat:**     new feature
- **fix:**      bug fix
- **refactor:** refactoring production code
- **style:**    formatting, missing semi colons, etc; no code change
- **docs:**     changes to documentation
- **test:**     adding or refactoring tests; no production code change
- **chore:**    updating grunt tasks etc; no production code change

The `<scope>` is optional but we encourage to use them for better 
separation of commits within a branch. Example values could be: assets, server, build, general

For example:

    feat(server): Implement changeOrigin on AssetAPI

The commit message should contain a short explanaition what the commited change is doing. There is no need in repeating t
the change itself since it's self explainatory by the code change.

Please refer to [this article](https://chris.beams.io/posts/git-commit/#imperative) to to see how to write good commit messages.
