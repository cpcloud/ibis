use std::convert::From;
use std::fmt;
use std::fmt::Debug;
use std::ops::{Add, AddAssign, Div};

use super::indextree::{first_node, last_node};
use super::{Fanout, Level};

pub trait AssociativeAggregate<Inputs, Output> {
    fn new() -> Self;
    fn step(&mut self, inputs: &Inputs) -> ();
    fn finalize(self) -> Option<Output>;
    fn combine_mut(&mut self, other: &Self) -> ();
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct Count {
    count: usize,
}

impl Count {
    fn new() -> Self {
        Count { count: 0 }
    }
}

impl fmt::Display for Count {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<Input1> AssociativeAggregate<(Option<Input1>,), usize> for Count {
    fn new() -> Self {
        Count::new()
    }

    fn step(&mut self, inputs: &(Option<Input1>,)) -> () {
        if let Some(_) = inputs.0 {
            self.count += 1;
        }
    }

    fn finalize(self) -> Option<usize> {
        if self.count != 0 {
            Some(self.count)
        } else {
            None
        }
    }

    fn combine_mut(&mut self, other: &Self) -> () {
        self.count += other.count;
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Sum<Output>
where
    Output: Add + Clone + Sized + Default + Copy,
{
    count: usize,
    total: Output,
}

impl<Output> fmt::Display for Sum<Output>
where
    Output: Add + Clone + Sized + Debug + Default + Copy,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Total<Output>
where
    Output: Add + Clone + Sized + Default + Copy,
{
    sum: Sum<Output>,
}

impl<Output> fmt::Display for Total<Output>
where
    Output: Add + Clone + Sized + Debug + Default + Copy,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<Output> Sum<Output>
where
    Output: Add + AddAssign + Clone + Default + Copy,
{
    fn new() -> Self {
        Sum {
            count: 0,
            total: Output::default(),
        }
    }
}

impl<Output> Total<Output>
where
    Output: Add + AddAssign + Clone + Default + Copy,
{
    fn new() -> Self {
        Total { sum: Sum::new() }
    }
}

impl<Input1, Output> AssociativeAggregate<(Option<Input1>,), Output>
    for Sum<Output>
where
    Input1: Add + AddAssign + Clone,
    Output: Add + From<Input1> + Clone + AddAssign + Default + Copy,
{
    fn new() -> Self {
        Sum::new()
    }

    fn step(&mut self, inputs: &(Option<Input1>,)) -> () {
        if let Some(ref value) = inputs.0 {
            self.total += Output::from(value.clone());
            self.count += 1;
        }
    }

    fn finalize(self) -> Option<Output> {
        if self.count != 0 {
            Some(self.total)
        } else {
            None
        }
    }

    fn combine_mut(&mut self, other: &Self) -> () {
        self.total += other.total.clone();
        self.count += other.count;
    }
}

impl<Input1, Output> AssociativeAggregate<(Option<Input1>,), Output>
    for Total<Output>
where
    Input1: Add + AddAssign + Clone,
    Output: Add + From<Input1> + Clone + AddAssign + Default + Copy,
{
    fn new() -> Self {
        Total::new()
    }

    fn step(&mut self, inputs: &(Option<Input1>,)) -> () {
        self.sum.step(inputs);
    }

    fn finalize(self) -> Option<Output> {
        if let Some(value) = self.sum.finalize() {
            Some(value)
        } else {
            Some(Output::default())
        }
    }

    fn combine_mut(&mut self, other: &Self) -> () {
        self.sum.combine_mut(&other.sum);
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Mean {
    sum: Sum<f64>,
}

impl<Input1> AssociativeAggregate<(Option<Input1>,), f64> for Mean
where
    Input1: Add + Div + AddAssign + Clone,
    f64: From<Input1>,
{
    fn new() -> Self {
        Mean { sum: Sum::new() }
    }

    fn step(&mut self, inputs: &(Option<Input1>,)) -> () {
        self.sum.step(inputs);
    }

    fn finalize(self) -> Option<f64> {
        let sum = self.sum;
        let total = sum.total;
        let count = sum.count;
        if count != 0 {
            Some(total / count as f64)
        } else {
            None
        }
    }

    fn combine_mut(&mut self, other: &Self) -> () {
        self.sum.combine_mut(&other.sum);
    }
}

pub struct Levels<'a, Aggregate> {
    nodes: &'a Vec<Aggregate>,
    level: Level,
    height: Level,
    fanout: Fanout,
}

impl<'a, Aggregate> Levels<'a, Aggregate> {
    pub fn new(
        nodes: &'a Vec<Aggregate>,
        level: Level,
        height: Level,
        fanout: Fanout,
    ) -> Self {
        Levels {
            nodes,
            level,
            height,
            fanout,
        }
    }
}

impl<'a, Aggregate> Iterator for Levels<'a, Aggregate> {
    type Item = &'a [Aggregate];

    fn next(&mut self) -> Option<Self::Item> {
        let level = self.level;
        let fanout = self.fanout;
        let result = if level >= self.height {
            None
        } else {
            let start = first_node(level, fanout);
            let stop = last_node(level, fanout);
            Some(&self.nodes[start..stop])
        };
        self.level += 1;
        result
    }
}
