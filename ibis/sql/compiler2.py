"""Version 2 of the ibis SQL compiler

Dependencies:

* Hashable expressions
* An "unsafe" expression substitution function that will recursively replace
  all the things
* Remove the notion of a "blocking" operation.

#. Expression comes in as an unoptimized tree

#. Optimize the expression (no-op right now, these are potentially interesting
   to implement)
   * Predicate fusion (collapse predicates across subqueries)
   * Projection fusion (collapse projections across subqueries)
   * Push down projections (change all `*` selections into column references if
                            possible)
   * Constant propagation (replace all references to literals with the literal)
   * Constant folding (evaluate any constants)
   * Extract subqueries (find all subqueries used more than once ->
                         form a CTE -> replace all subqueries with the CTE)

#. Convert to a simpler tree consisting of select, filter, column
   #. At this point our expression has been type checked

#. Provide aliases to unnamed subqueries
#. The query is compiled into a string
"""
