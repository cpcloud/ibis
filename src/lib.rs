// Source adopted from
// https://github.com/tildeio/helix-website/blob/master/crates/word_count/src/lib.rs

extern crate pyo3;
extern crate ndarray;
extern crate numpy;

use numpy::{PyArrayDyn};
use pyo3::prelude::*;

#[pymodinit]
fn rwindow(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    #[pyfn(m, "sum_array")]
    fn sum_array(x: &PyArrayDyn<f64>) -> f64 {
        x.as_array().sum()
    }
    Ok(())
}
