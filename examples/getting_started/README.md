# Getting Started

This section is still under construction. 
At present, it contains three examples:

- A very basic example, which primarily demonstrates how the framework works.
- A simple example on how CoSy-Luigi models variance by inheritance.
- An example that shows how running `build` and the caching looks.

Some interesting quirks: 

- When adding an abstract class or a class that directly inherits from ABC to a repository, it is expanded to all of its concrete implementations. 
- This behaviour can be manually replicated by calling `get_all_variants` on the class. 