type ILogger = {
	log(moduleName: string, ...args: Parameters<Console["log"]>): void;
	error(moduleName: string, ...args: Parameters<Console["log"]>): void;
};
