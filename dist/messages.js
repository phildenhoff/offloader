export const requireLabel = (f) => (event) => {
	if (!("label" in event)) {
		console.warn(`Discarding invalid event ${event}`);
		return;
	}
	f(event);
};
