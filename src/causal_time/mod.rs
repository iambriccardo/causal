use std::cmp::max;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::vec::IntoIter;

use itertools::Itertools;

use crate::causal_time::ClockComparison::{Concurrent, Equal, Greater, Less};

#[derive(PartialEq, Debug)]
pub enum ClockComparison {
    Equal,
    Less,
    Greater,
    Concurrent,
}

#[derive(Debug)]
pub struct VectorClock<T> {
    vector: HashMap<T, i32>,
}

impl<T> VectorClock<T>
    where T: Display + Eq + Hash + Copy {
    pub fn init() -> VectorClock<T> {
        VectorClock {
            vector: HashMap::new(),
        }
    }

    pub fn increment(&mut self, replica_id: T) {
        *self.vector.entry(replica_id).or_insert(0) += 1;
    }

    pub fn merge(&mut self, _: T, other_vector: &VectorClock<T>) {
        //self.increment(replica_id);

        self.replicas()
            .chain(other_vector.replicas())
            .unique()
            .for_each(|replica_id| {
                let a = self.vector.get(&replica_id)
                    .or(Some(&0))
                    .unwrap()
                    .clone();
                let b = other_vector.vector.get(&replica_id)
                    .or(Some(&0))
                    .unwrap()
                    .clone();

                self.vector.insert(replica_id, max(a, b));
            });
    }

    pub fn compare(&self, other_vector: &VectorClock<T>) -> ClockComparison {
        self.replicas()
            .chain(other_vector.replicas())
            .unique()
            .fold(Equal, |prev_cmp, replica_id| {
                let a = self.vector.get(&replica_id)
                    .or(Some(&0))
                    .unwrap();
                let b = other_vector.vector.get(&replica_id)
                    .or(Some(&0))
                    .unwrap();

                match prev_cmp {
                    Equal if a < b => Less,
                    Equal if a > b => Greater,
                    Less if a > b => Concurrent,
                    Greater if a < b => Concurrent,
                    _ => prev_cmp
                }
            })
    }

    fn replicas(&self) -> IntoIter<T> {
        let mut vec = vec![];
        vec.extend(self.vector.keys().clone());
        vec.into_iter()
    }
}

impl<T: Display + Eq + Hash + Copy> Clone for VectorClock<T> {
    fn clone(&self) -> Self {
        VectorClock {
            vector: self.vector.clone(),
        }
    }
}

impl<T: Display + Eq + Hash + Copy> Eq for VectorClock<T> {}

impl<T: Display + Eq + Hash + Copy> PartialEq<Self> for VectorClock<T> {
    fn eq(&self, other: &Self) -> bool {
        self.compare(other) == Equal
    }
}

impl<T: Display + Eq + Hash + Copy> Display for VectorClock<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut output = String::from("");

        output.push_str("[");
        for (replica_id, clock) in &self.vector {
            output.push_str(&format!("(r:{},s:{})", replica_id, clock));
        }
        output.push_str("]");

        write!(f, "{}", output)
    }
}

impl<T: Display + Eq + Hash + Copy> Hash for VectorClock<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for (replica, clock) in &self.vector {
            replica.hash(state);
            clock.hash(state);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::f32::consts::E;

    use crate::{Greater, VectorClock};
    use crate::vector_clocks::ClockComparison::{Concurrent, Equal, Greater, Less};

    #[test]
    fn increment() {
        let mut clock_1 = VectorClock::init();
        clock_1.increment(0);

        let mut expected_clock = HashMap::new();
        expected_clock.insert(0, 1);

        assert_eq!(clock_1.vector, expected_clock);
    }

    #[test]
    fn merge() {
        let mut clock_1 = VectorClock::init();
        clock_1.increment(0);

        let mut clock_2 = VectorClock::init();
        clock_2.increment(1);
        clock_2.increment(1);

        clock_1.merge(0, &clock_2);

        let mut expected_clock = HashMap::new();
        expected_clock.insert(0, 2);
        expected_clock.insert(1, 2);

        assert_eq!(clock_1.vector, expected_clock);
    }

    #[test]
    fn merge_complex() {
        let mut clock_1 = VectorClock::init();
        let mut clock_2 = VectorClock::init();

        clock_1.increment(0);
        clock_2.increment(1);

        clock_1.merge(0, &clock_2);
        clock_2.merge(1, &clock_1);

        let mut expected_clock_1 = HashMap::new();
        expected_clock_1.insert(0, 2);
        expected_clock_1.insert(1, 1);

        assert_eq!(clock_1.vector, expected_clock_1);

        let mut expected_clock_2 = HashMap::new();
        expected_clock_2.insert(0, 2);
        expected_clock_2.insert(1, 2);
        assert_eq!(clock_2.vector, expected_clock_2);
    }

    #[test]
    fn compare_less() {
        let mut clock_1 = VectorClock::init();
        clock_1.increment(0);

        let mut clock_2 = VectorClock::init();
        clock_2.merge(1, &clock_1);

        assert_eq!(
            clock_1.compare(&clock_2),
            Less
        );
    }

    #[test]
    fn compare_greater() {
        let mut clock_1 = VectorClock::init();
        clock_1.increment(0);

        let mut clock_2 = VectorClock::init();
        clock_2.merge(1, &clock_1);

        assert_eq!(
            clock_2.compare(&clock_1),
            Greater
        );
    }

    #[test]
    fn compare_equal() {
        let mut clock_1: VectorClock<usize> = VectorClock::init();

        let mut clock_2: VectorClock<usize> = VectorClock::init();

        assert_eq!(
            clock_1.compare(&clock_2),
            Equal
        );
    }

    #[test]
    fn compare_concurrent() {
        let mut clock_1 = VectorClock::init();
        clock_1.increment(0);

        let mut clock_2 = VectorClock::init();
        clock_2.increment(1);

        assert_eq!(
            clock_1.compare(&clock_2),
            Concurrent
        );
    }
}