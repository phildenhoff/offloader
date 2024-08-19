interface ILogEngine {
	log(...args: unknown[]): void;
	error(...args: unknown[]): void;
}
class ConsoleLogEngine implements ILogEngine {
	log: ILogEngine["log"] = (...args) => {
		console.log(...args);
	};
	error: ILogEngine["error"] = (...args) => {
		console.log(...args);
	};
}
class DummyEngine implements ILogEngine {
	log() {
		undefined;
	}
	error() {
		undefined;
	}
}

export class Logger {
	engine: ILogEngine;

	constructor() {
		if (process.env["NODE_ENV"] !== "production") {
			this.engine = new ConsoleLogEngine();
		} else {
			this.engine = new DummyEngine();
		}
	}

	log(moduleName: string, ...args: unknown[]) {
		this.engine.log(`[${moduleName}]:`, ...args);
	}

	error(moduleName: string, ...args: unknown[]) {
		this.engine.error(`[${moduleName}]:`, ...args);
	}
}
