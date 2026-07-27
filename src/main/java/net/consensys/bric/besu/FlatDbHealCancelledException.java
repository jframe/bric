package net.consensys.bric.besu;

/**
 * Thrown by {@link FlatDbHealer#heal} when a cancellation has been requested (e.g. the JVM shutdown
 * hook reacting to Ctrl-C) and the heal has stopped at a safe point between batches. Distinct from a
 * genuine failure so callers can report a clean cancellation rather than an error.
 */
public class FlatDbHealCancelledException extends RuntimeException {
    public FlatDbHealCancelledException() {
        super("Flat DB heal cancelled.");
    }
}
