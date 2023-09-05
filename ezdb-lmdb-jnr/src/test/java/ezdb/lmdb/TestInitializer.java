package ezdb.lmdb;

import org.burningwave.core.assembler.StaticComponentContainer;

public class TestInitializer {

	private static boolean initialized = false;

	public static synchronized void init() {
		if (!initialized) {
			StaticComponentContainer.Modules.exportAllToAll();
			initialized = true;
		}
	}

}
