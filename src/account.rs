use super::common::*;
use serde::Serialize;
use thiserror::Error;

#[derive(Default, Clone, Copy, Serialize, Debug, PartialEq)]
pub struct AccountInner<ST> {
    pub available: Value,
    pub held: Value,
    _marker: std::marker::PhantomData<ST>,
}

#[derive(Serialize, Debug, PartialEq)]
pub enum Account {
    Active(AccountInner<Active>),
    Frozen(AccountInner<Frozen>),
}

impl Default for Account {
    fn default() -> Self {
        Account::Active(AccountInner::default())
    }
}

#[derive(Default, Debug, PartialEq, Clone, Copy)]
pub struct Active;
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct Frozen;

#[derive(Debug, Error)]
pub enum AccountError {
    #[error("not enough funds")]
    NotEnoughFunds,
}

impl AccountInner<Active> {
    pub fn withdraw(&self, amount: Value) -> Result<Self, AccountError> {
        if self.available < amount {
            return Err(AccountError::NotEnoughFunds);
        }

        Ok(Self {
            available: self.available - amount,
            ..*self
        })
    }

    pub fn deposit(&self, amount: Value) -> Result<Self, AccountError> {
        Ok(Self {
            available: self.available + amount,
            ..*self
        })
    }

    pub fn freeze_funds(&self, amount: Value) -> Result<Self, AccountError> {
        // It could happen that the client has already spent funds which are now disputed.
        // In such cases, assume the balance can go negative to reflect a debit with the bank.
        Ok(Self {
            available: self.available - amount,
            held: self.held + amount,
            ..*self
        })
    }

    pub fn release_funds(&self, amount: Value) -> Result<Self, AccountError> {
        if self.held < amount {
            return Err(AccountError::NotEnoughFunds);
        }
        Ok(Self {
            available: self.available + amount,
            held: self.held - amount,
            ..*self
        })
    }

    pub fn chargeback(&self, amount: Value) -> Result<Self, AccountError> {
        if self.held < amount {
            return Err(AccountError::NotEnoughFunds);
        }
        Ok(Self {
            held: self.held - amount,
            ..*self
        })
    }

    pub fn freeze(&self) -> AccountInner<Frozen> {
        AccountInner {
            _marker: std::marker::PhantomData::<Frozen>,
            held: self.held,
            available: self.available,
        }
    }
}

impl From<AccountInner<Frozen>> for Account {
    fn from(from: AccountInner<Frozen>) -> Self {
        Self::Frozen(from)
    }
}

impl From<AccountInner<Active>> for Account {
    fn from(from: AccountInner<Active>) -> Self {
        Self::Active(from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Account {
        pub fn available(&self) -> Value {
            match self {
                Account::Active(i) => i.available,
                Account::Frozen(i) => i.available,
            }
        }

        pub fn held(&self) -> Value {
            match self {
                Account::Active(i) => i.held,
                Account::Frozen(i) => i.held,
            }
        }
    }

    #[test]
    fn test_withdraw_no_balance() {
        let account = AccountInner::<Active>::default();
        assert!(account.withdraw(Value::ONE).is_err());
    }

    #[test]
    fn test_release_no_funds() {
        let account = AccountInner::<Active>::default();
        assert!(account.release_funds(Value::ONE).is_err());
    }

    #[test]
    fn test_chargeback_no_funds() {
        let account = AccountInner::<Active>::default();
        assert!(account.chargeback(Value::ONE).is_err());
    }
}
