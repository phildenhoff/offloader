export type LabelledMessageEvent<
	T extends Record<string, unknown> = Record<string, never>,
> = { label: string } & T;

export const requireLabel =
	<T extends Record<string, unknown>>(
		f: (event: LabelledMessageEvent<T>) => unknown,
	) =>
	(event: object): void => {
		if (!("label" in event)) {
			console.warn(`Discarding invalid event ${event}`);
			return;
		}
		f(event as LabelledMessageEvent<T>);
	};
