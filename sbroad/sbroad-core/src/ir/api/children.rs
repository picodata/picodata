use std::ops::{Index, Range, RangeFrom, RangeFull};

use crate::ir::node::NodeId;

#[derive(Debug, Clone)]
pub enum Children<'r> {
    None,
    Single(&'r NodeId),
    Couple(&'r NodeId, &'r NodeId),
    Many(&'r [NodeId]),
}

impl<'r> Index<usize> for Children<'r> {
    type Output = NodeId;

    fn index(&self, idx: usize) -> &Self::Output {
        match self {
            Children::None => panic!("node has no children!"),
            Children::Single(i) => {
                assert!(idx == 0, "node has one child!");
                i
            }
            Children::Couple(l, r) => match idx {
                0 => l,
                1 => r,
                _ => panic!("node has only two children!"),
            },
            Children::Many(i) => &i[idx],
        }
    }
}

impl<'r> Index<Range<usize>> for Children<'r> {
    type Output = [NodeId];

    fn index(&self, range: Range<usize>) -> &Self::Output {
        match self {
            Children::Many(i) => &i[range],
            _ => panic!("range indexing is only for nodes with additional children: Join, Selection, Having"),
        }
    }
}

impl<'r> Index<RangeFrom<usize>> for Children<'r> {
    type Output = [NodeId];

    fn index(&self, range: RangeFrom<usize>) -> &Self::Output {
        &self[range.start..self.len()]
    }
}

impl<'r> Index<RangeFull> for Children<'r> {
    type Output = [NodeId];

    fn index(&self, _: RangeFull) -> &Self::Output {
        &self[0..self.len()]
    }
}

impl<'r> Children<'r> {
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Children::None => 0,
            Children::Single(_) => 1,
            Children::Couple(_, _) => 2,
            Children::Many(inner) => inner.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn to_vec(&self) -> Vec<NodeId> {
        match self {
            Children::None => vec![],
            Children::Single(i) => vec![**i],
            Children::Couple(l, r) => vec![**l, **r],
            Children::Many(inner) => inner.to_vec(),
        }
    }

    #[must_use]
    pub fn get(&self, idx: usize) -> Option<&'r NodeId> {
        if idx >= self.len() {
            return None;
        }
        let res = match self {
            Children::Single(i) => i,
            Children::Couple(a, b) => {
                if idx == 0 {
                    a
                } else {
                    b
                }
            }
            Children::Many(i) => return i.get(idx),
            Children::None => return None,
        };
        Some(res)
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct ChildrenIter<'c> {
    inner: &'c Children<'c>,
    step: usize,
}

impl<'c> Iterator for ChildrenIter<'c> {
    type Item = &'c NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        let child = match self.inner {
            Children::None => return None,
            Children::Single(i) => {
                if self.step > 0 {
                    return None;
                }
                i
            }
            Children::Couple(l, r) => match self.step {
                0 => l,
                1 => r,
                _ => return None,
            },
            Children::Many(i) => {
                if self.step >= i.len() {
                    return None;
                }
                &i[self.step]
            }
        };
        self.step += 1;
        Some(child)
    }
}

impl<'r> Children<'r> {
    #[must_use]
    pub fn iter(&self) -> ChildrenIter<'_> {
        ChildrenIter {
            inner: self,
            step: 0,
        }
    }
}

impl<'r> IntoIterator for &'r Children<'r> {
    type Item = &'r NodeId;

    type IntoIter = ChildrenIter<'r>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub enum MutChildren<'r> {
    None,
    Single(&'r mut NodeId),
    Couple(&'r mut NodeId, &'r mut NodeId),
    Many(&'r mut [NodeId]),
}

impl<'r> MutChildren<'r> {
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            MutChildren::None => 0,
            MutChildren::Single(_) => 1,
            MutChildren::Couple(_, _) => 2,
            MutChildren::Many(inner) => inner.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn to_vec(&self) -> Vec<NodeId> {
        match self {
            MutChildren::None => vec![],
            MutChildren::Single(i) => vec![**i],
            MutChildren::Couple(l, r) => vec![**l, **r],
            MutChildren::Many(inner) => inner.to_vec(),
        }
    }

    // TODO: change self to &mut self, rust somewhy tells that
    // that instance lifetime '1 must be bigger than 'r and we
    // return '1
    #[must_use]
    pub fn get_mut(self, idx: usize) -> Option<&'r mut NodeId> {
        if idx >= self.len() {
            return None;
        }
        Some(match self {
            MutChildren::Single(i) => i,
            MutChildren::Couple(a, b) => {
                if idx == 0 {
                    a
                } else {
                    b
                }
            }
            MutChildren::Many(i) => return i.get_mut(idx),
            MutChildren::None => return None,
        })
    }

    #[must_use]
    pub fn split_first(self) -> Option<(&'r mut NodeId, MutChildren<'r>)> {
        let res = match self {
            MutChildren::None => return None,
            MutChildren::Single(i) => (i, MutChildren::None),
            MutChildren::Couple(l, r) => (l, MutChildren::Single(r)),
            MutChildren::Many(i) => {
                let (elem, next) = i.split_first_mut()?;
                (elem, MutChildren::Many(next))
            }
        };
        Some(res)
    }
}

pub struct MutChildrenIter<'c> {
    inner: Option<MutChildren<'c>>,
}

impl<'c> Iterator for MutChildrenIter<'c> {
    type Item = &'c mut NodeId;

    #[must_use]
    fn next(&mut self) -> Option<Self::Item> {
        let inner = std::mem::take(&mut self.inner);
        if let Some(inner) = inner {
            let (elem, next) = inner.split_first()?;
            self.inner = Some(next);
            Some(elem)
        } else {
            None
        }
    }
}

impl<'r> MutChildren<'r> {
    #[must_use]
    pub fn iter_mut(self) -> MutChildrenIter<'r> {
        MutChildrenIter { inner: Some(self) }
    }
}

impl<'r> IntoIterator for MutChildren<'r> {
    type Item = &'r mut NodeId;

    type IntoIter = MutChildrenIter<'r>;

    #[must_use]
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}
