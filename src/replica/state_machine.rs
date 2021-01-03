pub trait StateMachine {

}

pub struct NoOpStateMachine {
    // nothing
}

impl NoOpStateMachine {
    pub fn new() -> Self {
        NoOpStateMachine {}
    }
}

impl StateMachine for NoOpStateMachine {

}