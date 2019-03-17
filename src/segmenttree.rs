use super::indextree::reprtree;
use super::indextree::IndexTree;
use super::stupidb::{AssociativeAggregate, Levels};
use bit_set::BitSet;
use std::collections::vec_deque::VecDeque;
use std::fmt;
use std::fmt::Display;
use std::marker::PhantomData;
use std::ops::{Add, Div};

struct SegmentTree<Inputs, Output, Aggregate>
where
    Aggregate: AssociativeAggregate<Inputs, Output>,
{
    nodes: Vec<Aggregate>,
    fanout: usize,
    _phantom1: PhantomData<Inputs>,
    _phantom2: PhantomData<Output>,
}

impl<Inputs, Output, Aggregate> fmt::Display
    for SegmentTree<Inputs, Output, Aggregate>
where
    Aggregate: AssociativeAggregate<Inputs, Output> + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", reprtree(&self.nodes, self.fanout, "    "))
    }
}

impl<Inputs, Output, Aggregate> SegmentTree<Inputs, Output, Aggregate>
where
    Aggregate: Clone + AssociativeAggregate<Inputs, Output>,
{
    fn new(leaves: &Vec<Inputs>, fanout: usize) -> Self {
        make_segment_tree::<Inputs, Output, Aggregate>(leaves, fanout)
    }

    fn levels(&self) -> Levels<Aggregate> {
        let nodes = &self.nodes;
        let fanout = self.fanout;
        let height = (nodes.len() as f64).log(fanout as f64).ceil() as usize;
        let level = 0;
        Levels::new(nodes, level, height, fanout)
    }

    fn query(&self, mut begin: usize, mut end: usize) -> Option<Output> {
        let fanout = self.fanout;
        let mut aggregate = Aggregate::new();
        for level in self.levels() {
            let mut parent_begin = begin.div(fanout);
            let mut parent_end = end.div(fanout);
            if parent_begin == parent_end {
                for item in &level[begin..end] {
                    aggregate.combine_mut(item);
                }
                return aggregate.finalize();
            }

            let group_begin = parent_begin * fanout;
            if begin != group_begin {
                let limit = group_begin + fanout;
                for item in &level[begin..limit] {
                    aggregate.combine_mut(item);
                }
                parent_begin += 1;
            }
            let group_end = parent_end * fanout;
            if end != group_end {
                for item in &level[group_end..end] {
                    aggregate.combine_mut(item);
                }
            }
            begin = parent_begin;
            end = parent_end;
        }

        None
    }
}

pub fn make_segment_tree<Inputs, Output, Aggregate>(
    leaves: &Vec<Inputs>,
    fanout: usize,
) -> SegmentTree<Inputs, Output, Aggregate>
where
    Aggregate: AssociativeAggregate<Inputs, Output> + Clone,
{
    let number_of_leaves = leaves.len();
    let height =
        (number_of_leaves as f64).log(fanout as f64).ceil().add(1.0) as usize;
    let index_tree = IndexTree::new(height, fanout);
    let mut segment_tree_nodes: Vec<Aggregate> = vec![];
    for _ in 0..index_tree.len() {
        segment_tree_nodes.push(Aggregate::new());
    }
    let mut queue: VecDeque<_> = index_tree.leaves().collect();

    for (leaf_index, args) in queue.iter().zip(leaves) {
        let leaf = &mut segment_tree_nodes[*leaf_index];
        leaf.step(args);
    }

    let mut seen = BitSet::with_capacity(index_tree.len());

    while !queue.is_empty() {
        let node_index = queue.pop_front().unwrap();
        if !seen.contains(node_index) {
            let node_agg = segment_tree_nodes[node_index].clone();
            let parent = index_tree.parent(node_index);
            let parent_agg = &mut segment_tree_nodes[parent];
            parent_agg.combine_mut(&node_agg);
            seen.insert(node_index);
            if parent != 0 {
                queue.push_back(parent);
            }
        }
    }

    SegmentTree {
        nodes: segment_tree_nodes,
        fanout: fanout,
        _phantom1: PhantomData,
        _phantom2: PhantomData,
    }
}

#[cfg(test)]
mod tests {
    use super::super::stupidb;
    use super::*;

    #[test]
    fn test_repr_segtree() {
        type Aggregate = stupidb::Sum<usize>;
        type Inputs = (Option<usize>,);
        type Output = usize;
        let leaves = vec![(Some(1),), (Some(2),), (Some(3),), (Some(4),)];
        let fanout = 2;
        let segtree =
            make_segment_tree::<Inputs, Output, Aggregate>(&leaves, fanout);
        let expected = "\
|-- Sum { count: 4, total: 10 }
    |-- Sum { count: 2, total: 3 }
        |-- Sum { count: 1, total: 1 }
        |-- Sum { count: 1, total: 2 }
    |-- Sum { count: 2, total: 7 }
        |-- Sum { count: 1, total: 3 }
        |-- Sum { count: 1, total: 4 }";
        let result = format!("{}", segtree);
        assert_eq!(result, expected);
    }
}
