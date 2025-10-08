### Objective of This `io` Package
- Provide integration with external systems
- Making sure the users of this package is able to mock the classes of this package for unit testing purpose

### Usage information
Any code in this repository should go via this `io` package layer which will help to have a single place to fix bugs, do upgrades etc.

### Things to Keep in Mind for Development of This Package
- ***This package should not contain any business logic***
- As this package integrate with external system, ***writing unit test won't be possible***. Though integration tests can be added.
- The functionalities provided by this package should be primitive in nature
