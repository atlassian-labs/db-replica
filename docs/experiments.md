## Experiments

`Experiments` is an API to change the behaviour of the library [on the fly](https://martinfowler.com/articles/feature-toggles.html).
The API is transparent, you need to `opt-in` to take control over the change.

### Why

This technique allows for safer rollouts of features and bugfixes than just bumping a library.

### How

You can inject your implementation of `Experiment` via the `DualConnection` builder.


## Current experiments

none



Experiments are short living. It's expected to remove it with the next releases.
