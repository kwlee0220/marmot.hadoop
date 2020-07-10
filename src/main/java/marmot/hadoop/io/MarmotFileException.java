package marmot.hadoop.io;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFileException extends RuntimeException {
	public MarmotFileException(String details) {
		super(details);
	}

	public MarmotFileException(Throwable cause) {
		super(cause);
	}

	public MarmotFileException(String details, Throwable cause) {
		super(details, cause);
	}
}
