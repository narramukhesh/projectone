Contributing
============

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

## Types of Contributions

### Reporting Bugs

Report bugs at https://github.com/narramukhesh/projectone/issues.

If you are reporting a bug, please include:

* Description of the bug and details.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

### Fix Bugs

Look through the Jira board with title Bug under data platform epic tag will be taken into
consideration.

### Implement Features

Look through the Jira board with title Feature under data platform epic tag will be taken into
consideration.

### Write Documentation
Please include the docstrings, comment algorithm in place explaining the code,
Create README.md file for every module which is independent.
REAME.md should explain What, How, Why about code, module and include the links for references.


## Get Started!

Ready to contribute? Here's how to set up `projectone` for local development.

1. Fork the `projectone` repo on GitHub.
2. Clone your fork locally::

    $ git clone https://github.com/narramukhesh/projectone.git
3. For Local development, please use the docker-images
4. Create a branch for local development::

    $ git checkout -b {Type of branch}/ISS-{Issue number}

   Now you can make your changes locally.

   #### Type of branches:
    1. feat: A new feature.
    2. fix: A bug fix.
    3. docs: Documentation changes.
    4. style: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc).
    5. refactor: A code change that neither fixes a bug nor adds a feature.
    6. perf: A code change that improves performance.
    7. test: Changes to the test framework.
    8. build: Changes to the build process or tools.

5. When you're done making changes, where pre-commit hooks will format and check the commit messages
6. Before commiting the changes, need to enable the pre-commit which requires python enviornment to be installed
    
    Use conda environment setup and install pre-commit python package
    ```sh
     conda create -n "<environment name>" python==<python_version>
     pip install pre-commit
    ```
    If above pre-commit package not installed, you will receive error because of enabling of git hooks 
7. Commit your changes and push your branch to GitHub::
    ```
    $ git add .
    $ git commit -m "{Type of changes}: {some messages}
                    {description}
                    "
    $ git push origin {Type of branch}/{Jira ticket name}
    ```
8. Submit a pull request through the Gitlab

## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests and description.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.md
3. While merging to default branch, have title as version which is helful for the releases.

## Deploying

Gitlab CI/CD will take care of deployment and releasing of packages, Currently CI follows parent-child pipeline strategy.
