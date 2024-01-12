module.exports = {
    allowNumericalError: (process.env.ALLOW_NUMERICAL_ERROR === 'true') || false,
    allowNumericalRange: Number(process.env.ALLOW_NUMERICAL_RANGE) || 10,
    schemaTablePlayers: process.env.SCHEMA_TABLE_PLAYERS || 'wallet.players'
}