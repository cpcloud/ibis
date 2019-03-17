extern crate bit_set;
extern crate ndarray;
extern crate numpy;
extern crate pyo3;

type Level = usize;
type Fanout = usize;
type Node = usize;
type Start = usize;
type Stop = usize;

pub mod indextree;
pub mod segmenttree;
pub mod stupidb;

use std::collections::HashMap;
use std::range::Range;

use numpy::PyArrayDyn;
use pyo3::prelude::*;

fn compute_window_frame_bounds(
    peers_range: Range<usize>,
    current_row: usize,
    row_id_in_partition: usize,
) -> (usize, usize) {
    (0, 0)
}

#[pymodinit]
fn pandaswindow(_py: Python, m: &PyModule) -> PyResult<()> {
    #[pyfn(m, "rolling_sum")]
    fn rolling_sum(
        py: Python,
        values: &PyArrayDyn<f64>,
        index: &PyArrayDyn<i64>,
        keys: &PyArrayDyn<str>,
        out: &mut PyArrayDyn<f64>,
    ) -> () {
        let inputs = values
            .as_array()
            .map(|value| (if value.is_nan() { None } else { Some(value) },));
        let fanout = 2;
        let segtree = segmenttree::make_segment_tree(inputs, fanout);
        let partitions: HashMap<String, Range<usize>> = HashMap::new();
        // build partitions
        for key in keys {
            for row in partitions.get(key) {
                let (start, end) = compute_window_frame_bounds(
                    peers_range,
                    current_row,
                    row_id_in_partition,
                    order_by_columns,
                );
                out[i] = segtree.query(start, end);
            }
        }
    }
    Ok(())
}
