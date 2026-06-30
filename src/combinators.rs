// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::{Either2, Either3};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub(crate) struct PollAllJoin<F: Future> {
    futures: Vec<Option<Pin<Box<F>>>>,
    outputs: Vec<Option<F::Output>>,
    remaining: usize,
}

impl<F: Future> PollAllJoin<F> {
    pub(crate) fn new(futures: Vec<F>) -> Self {
        let remaining = futures.len();
        Self {
            futures: futures.into_iter().map(|future| Some(Box::pin(future))).collect(),
            outputs: std::iter::repeat_with(|| None).take(remaining).collect(),
            remaining,
        }
    }
}

impl<F: Future> Unpin for PollAllJoin<F> {}

impl<F: Future> Future for PollAllJoin<F> {
    type Output = Vec<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        for index in 0..this.futures.len() {
            if this.futures[index].is_none() {
                continue;
            }

            let poll_result = {
                let future = this.futures[index].as_mut().expect("future slot should be occupied");
                future.as_mut().poll(cx)
            };

            if let Poll::Ready(output) = poll_result {
                this.outputs[index] = Some(output);
                this.futures[index] = None;
                this.remaining -= 1;
            }
        }

        if this.remaining == 0 {
            let outputs = std::mem::take(&mut this.outputs)
                .into_iter()
                .map(|output| output.expect("completed future should have output"))
                .collect();
            Poll::Ready(outputs)
        } else {
            Poll::Pending
        }
    }
}

pub(crate) struct PollAllJoin2<F1: Future, F2: Future> {
    f1: Option<Pin<Box<F1>>>,
    f2: Option<Pin<Box<F2>>>,
    output1: Option<F1::Output>,
    output2: Option<F2::Output>,
}

impl<F1: Future, F2: Future> PollAllJoin2<F1, F2> {
    pub(crate) fn new(f1: F1, f2: F2) -> Self {
        Self {
            f1: Some(Box::pin(f1)),
            f2: Some(Box::pin(f2)),
            output1: None,
            output2: None,
        }
    }
}

impl<F1: Future, F2: Future> Unpin for PollAllJoin2<F1, F2> {}

impl<F1: Future, F2: Future> Future for PollAllJoin2<F1, F2> {
    type Output = (F1::Output, F2::Output);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        if let Some(future) = &mut this.f1
            && let Poll::Ready(output) = future.as_mut().poll(cx)
        {
            this.output1 = Some(output);
            this.f1 = None;
        }

        if let Some(future) = &mut this.f2
            && let Poll::Ready(output) = future.as_mut().poll(cx)
        {
            this.output2 = Some(output);
            this.f2 = None;
        }

        if this.f1.is_none() && this.f2.is_none() {
            Poll::Ready((
                this.output1.take().expect("completed future should have output"),
                this.output2.take().expect("completed future should have output"),
            ))
        } else {
            Poll::Pending
        }
    }
}

pub(crate) struct PollAllJoin3<F1: Future, F2: Future, F3: Future> {
    f1: Option<Pin<Box<F1>>>,
    f2: Option<Pin<Box<F2>>>,
    f3: Option<Pin<Box<F3>>>,
    output1: Option<F1::Output>,
    output2: Option<F2::Output>,
    output3: Option<F3::Output>,
}

impl<F1: Future, F2: Future, F3: Future> PollAllJoin3<F1, F2, F3> {
    pub(crate) fn new(f1: F1, f2: F2, f3: F3) -> Self {
        Self {
            f1: Some(Box::pin(f1)),
            f2: Some(Box::pin(f2)),
            f3: Some(Box::pin(f3)),
            output1: None,
            output2: None,
            output3: None,
        }
    }
}

impl<F1: Future, F2: Future, F3: Future> Unpin for PollAllJoin3<F1, F2, F3> {}

impl<F1: Future, F2: Future, F3: Future> Future for PollAllJoin3<F1, F2, F3> {
    type Output = (F1::Output, F2::Output, F3::Output);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        if let Some(future) = &mut this.f1
            && let Poll::Ready(output) = future.as_mut().poll(cx)
        {
            this.output1 = Some(output);
            this.f1 = None;
        }

        if let Some(future) = &mut this.f2
            && let Poll::Ready(output) = future.as_mut().poll(cx)
        {
            this.output2 = Some(output);
            this.f2 = None;
        }

        if let Some(future) = &mut this.f3
            && let Poll::Ready(output) = future.as_mut().poll(cx)
        {
            this.output3 = Some(output);
            this.f3 = None;
        }

        if this.f1.is_none() && this.f2.is_none() && this.f3.is_none() {
            Poll::Ready((
                this.output1.take().expect("completed future should have output"),
                this.output2.take().expect("completed future should have output"),
                this.output3.take().expect("completed future should have output"),
            ))
        } else {
            Poll::Pending
        }
    }
}

pub(crate) struct Select2<F1: Future, F2: Future> {
    f1: Option<Pin<Box<F1>>>,
    f2: Option<Pin<Box<F2>>>,
}

impl<F1: Future, F2: Future> Select2<F1, F2> {
    pub(crate) fn new(f1: F1, f2: F2) -> Self {
        Self {
            f1: Some(Box::pin(f1)),
            f2: Some(Box::pin(f2)),
        }
    }
}

impl<F1: Future, F2: Future> Unpin for Select2<F1, F2> {}

impl<F1: Future, F2: Future> Future for Select2<F1, F2> {
    type Output = Either2<F1::Output, F2::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        if let Some(future) = &mut this.f1
            && let Poll::Ready(output) = future.as_mut().poll(cx)
        {
            return Poll::Ready(Either2::First(output));
        }

        if let Some(future) = &mut this.f2
            && let Poll::Ready(output) = future.as_mut().poll(cx)
        {
            return Poll::Ready(Either2::Second(output));
        }

        Poll::Pending
    }
}

pub(crate) struct Select3<F1: Future, F2: Future, F3: Future> {
    f1: Option<Pin<Box<F1>>>,
    f2: Option<Pin<Box<F2>>>,
    f3: Option<Pin<Box<F3>>>,
}

impl<F1: Future, F2: Future, F3: Future> Select3<F1, F2, F3> {
    pub(crate) fn new(f1: F1, f2: F2, f3: F3) -> Self {
        Self {
            f1: Some(Box::pin(f1)),
            f2: Some(Box::pin(f2)),
            f3: Some(Box::pin(f3)),
        }
    }
}

impl<F1: Future, F2: Future, F3: Future> Unpin for Select3<F1, F2, F3> {}

impl<F1: Future, F2: Future, F3: Future> Future for Select3<F1, F2, F3> {
    type Output = Either3<F1::Output, F2::Output, F3::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        if let Some(future) = &mut this.f1
            && let Poll::Ready(output) = future.as_mut().poll(cx)
        {
            return Poll::Ready(Either3::First(output));
        }

        if let Some(future) = &mut this.f2
            && let Poll::Ready(output) = future.as_mut().poll(cx)
        {
            return Poll::Ready(Either3::Second(output));
        }

        if let Some(future) = &mut this.f3
            && let Poll::Ready(output) = future.as_mut().poll(cx)
        {
            return Poll::Ready(Either3::Third(output));
        }

        Poll::Pending
    }
}
