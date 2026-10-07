package wagering

// FailureCode is a stable, documented code persisted on REJECTED/FAILED
// transactions and returned to providers. Values must never be renamed.
type FailureCode string

const (
	// FailureInsufficientFunds: a BET would leave the wallet negative.
	FailureInsufficientFunds FailureCode = "INSUFFICIENT_FUNDS"
	// FailureReversalInsufficientFunds: a ROLLBACK that debits (of a WIN or
	// REFUND) would leave the wallet negative. Distinct from BET on purpose.
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	// FailureWalletPlayerMismatch: walletId does not belong to playerId.
	FailureWalletPlayerMismatch FailureCode = "WALLET_PLAYER_MISMATCH"
	// FailureCurrencyMismatch: operation currency differs from wallet currency.
	FailureCurrencyMismatch FailureCode = "CURRENCY_MISMATCH"
	// FailureReferenceNotFound: the reference never appeared before the retry
	// limit or TTL expired.
	FailureReferenceNotFound FailureCode = "REFERENCE_NOT_FOUND"
	// FailureReferenceNotResolved: the reference exists but stayed pending
	// until the retry limit or TTL expired.
	FailureReferenceNotResolved FailureCode = "REFERENCE_NOT_RESOLVED"
	// FailureReferenceNotProcessed: the reference ended REJECTED or FAILED.
	FailureReferenceNotProcessed FailureCode = "REFERENCE_NOT_PROCESSED"
	// FailureReferenceMismatch: reference differs in player, wallet, currency or round.
	FailureReferenceMismatch FailureCode = "REFERENCE_MISMATCH"
	// FailureReferenceKindNotAllowed: e.g. REFUND of a WIN, ROLLBACK of a LOSS.
	FailureReferenceKindNotAllowed FailureCode = "REFERENCE_KIND_NOT_ALLOWED"
	// FailureReversalAmountMismatch: reversals are full; amounts must match.
	FailureReversalAmountMismatch FailureCode = "REVERSAL_AMOUNT_MISMATCH"
	// FailureReferenceAlreadyReversed: the reference already has a successful
	// REFUND or ROLLBACK.
	FailureReferenceAlreadyReversed FailureCode = "REFERENCE_ALREADY_REVERSED"
)
