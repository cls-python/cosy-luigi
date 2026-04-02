# Getting Started

This section is still under construction. 
At present, it contains two examples:

- A very basic example, which primarily demonstrates how Luigi works
- A simple example on how CoSy-Luigi models variance by inheritance. 

Some interesting quirks: 

- When adding an abstract class or a class that directly inherits from ABC to a repository, it is expanded to all of its concrete implementations. 
- This behaviour can be manually replicated by calling `get_all_variants` on the class. 