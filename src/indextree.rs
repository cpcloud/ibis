use std::fmt;
use std::fmt::Display;
use std::ops::{Add, Div, Sub};

use bit_set::BitSet;

use super::{Fanout, Level, Node, Start, Stop};

pub fn first_node(level: Level, fanout: Fanout) -> usize {
    fanout.pow(level as u32).sub(1).div(fanout.sub(1))
}

pub fn last_node(level: Level, fanout: Fanout) -> usize {
    fanout.pow(level.add(1) as u32).sub(1).div(fanout.sub(1))
}

pub fn reprtree<T>(nodes: &Vec<T>, fanout: Fanout, indent: &str) -> String
where
    T: Display,
{
    let num_nodes = nodes.len();
    let mut level_index_stack = vec![(0, 0)];
    let mut seen = BitSet::with_capacity(num_nodes);
    let mut node_repr_pieces: Vec<String> = vec![];

    while !level_index_stack.is_empty() {
        if let Some((level, node_index)) = level_index_stack.pop() {
            let node = &nodes[node_index];
            let node_repr_piece =
                format!("{}|-- {}", indent.repeat(level), node);
            node_repr_pieces.push(node_repr_piece);
            level_index_stack.extend(
                (0..fanout)
                    .rev()
                    .map(|i| fanout * node_index + i + 1)
                    .filter(|index| *index < num_nodes)
                    .map(|index| (level + 1, index)),
            );
            seen.insert(node_index);
        }
    }
    node_repr_pieces.join("\n")
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct IndexTree {
    height: Level,
    fanout: Fanout,
    start: Start,
    stop: Stop,
}

impl fmt::Display for IndexTree {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let nodes: Vec<_> = (self.start..self.stop).collect();
        write!(f, "{}", reprtree(&nodes, self.fanout, "    "))
    }
}

impl IndexTree {
    pub fn new(height: Level, fanout: Fanout) -> Self {
        IndexTree {
            height: height,
            fanout: fanout,
            start: 0,
            stop: fanout.pow(height as u32).sub(1).div(fanout.sub(1)),
        }
    }

    pub fn len(&self) -> usize {
        self.stop - self.start
    }

    pub fn leaves(&self) -> std::ops::Range<usize> {
        let fanout = self.fanout;
        let last_level = self.height.sub(1);
        let first = first_node(last_level, fanout);
        let last = last_node(last_level, fanout);
        first..last
    }

    pub fn parent(&self, node: Node) -> Node {
        if node != 0 {
            node.sub(1).div(self.fanout)
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repr_index_tree_fanout_2() {
        let index_tree = IndexTree::new(3, 2);
        let expected = "\
|-- 0
    |-- 1
        |-- 3
        |-- 4
    |-- 2
        |-- 5
        |-- 6";
        let result = format!("{}", index_tree);
        assert_eq!(result, expected);
    }
}
