export type LabelledMessageEvent<
	T extends Record<string, unknown> = Record<string, never>,
> = MessageEvent<{ label: string } & T>;

export const requireLabel =
	<T extends Record<string, unknown>>(
		f: (event: LabelledMessageEvent<T>) => unknown,
	) =>
	(event: MessageEvent): void => {
		if (!("label" in event.data)) {
			console.warn(`Discarding invalid event ${event}`);
			return;
		}
		f(event as LabelledMessageEvent<T>);
	};
