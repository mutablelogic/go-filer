// A meter chart, of Carbon Charts, of how a total is made up of groups,
// such as a volume's objects which are indexed, and which aren't

import { LitElement, type PropertyValues } from 'lit';
import { MeterChart, type ChartTabularData, type MeterChartOptions } from '@carbon/charts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// MeterGroup is a group of a meter, and its value
export interface MeterGroup {
    group: string;
    value: number;
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

export const MeterTag = 'gf-meter';

// MeterHeight is a meter's height, of its total, its bar, and its legend,
// as its chart has none of its own, of a parent of no height
const MeterHeight = '88px';

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

// MeterElement is a proportional meter chart of its groups, of their total,
// in units, such as events, whose chart is made once, and its data and
// options changed, as they are
export class MeterElement extends LitElement {
    static override properties = {
        groups: { attribute: false },
        colors: { attribute: false },
        descriptions: { attribute: false },
        total: { type: Number },
        unit: { type: String },
        label: { type: String },
    };

    // groups are in the order of the bar, and its legend, and colors are
    // their colours, of their groups, such as Carbon's, and label is what
    // the chart is, which a screen reader says, as it has no title
    declare groups: readonly MeterGroup[];
    declare colors: Readonly<Record<string, string>>;

    // descriptions are what the groups are, of their legend's items, when
    // they're pointed at
    declare descriptions: Readonly<Record<string, string>>;
    declare total: number;
    declare unit: string;
    declare label: string;
    private chart?: MeterChart;

    constructor() {
        super();
        this.groups = [];
        this.colors = {};
        this.descriptions = {};
        this.total = 0;
        this.unit = '';
        this.label = '';
    }

    // The chart is in the light DOM, as Carbon Charts' styles are global
    protected override createRenderRoot() {
        return this;
    }

    // A meter which is removed, and added again, is updated, so its chart,
    // which was destroyed, is made again, as Lit doesn't update an element
    // which is added again, without a change
    override connectedCallback() {
        super.connectedCallback();
        if (this.hasUpdated) {
            this.requestUpdate();
        }
    }

    override disconnectedCallback() {
        super.disconnectedCallback();
        this.clear();
    }

    protected override updated(changed: PropertyValues<this>) {
        // A meter of nothing has no chart, as Carbon Charts' scale of a total
        // of zero draws each group as half of the bar, and says so
        if (!(this.total > 0)) {
            this.clear();
            const empty = document.createElement('p');
            empty.className = 'gf-meter-empty';
            empty.textContent = this.unit ? `No ${this.unit}` : 'None';
            this.replaceChildren(empty);
            return;
        }
        const data = this.groups.map((g) => ({ ...g })) as ChartTabularData;
        const options = this.options();
        if (!this.chart) {
            const holder = document.createElement('div');
            this.replaceChildren(holder);
            this.chart = new MeterChart(holder, { data, options });
            this.chart.services.events.addEventListener('render-finished', () => this.describe());
            return;
        }
        if (changed.has('groups')) {
            this.chart.model.setData(data);
        }
        if (changed.has('total') || changed.has('unit') || changed.has('label') || changed.has('colors') || changed.has('groups')) {
            this.chart.model.setOptions(options);
        }
        if (changed.has('descriptions')) {
            this.describe();
        }
    }

    // clear destroys the chart, when there's one, and its elements
    private clear() {
        this.chart?.destroy();
        this.chart = undefined;
        this.replaceChildren();
    }

    // describe sets the title of each item of the legend, of its group's
    // description, as the legend is drawn again whenever the chart is
    private describe() {
        for (const item of this.querySelectorAll<HTMLElement>('.legend-item')) {
            const group = item.querySelector('p')?.textContent?.trim() ?? '';
            const description = Object.hasOwn(this.descriptions, group) ? this.descriptions[group] : '';
            if (description) {
                item.title = description;
            } else {
                item.removeAttribute('title');
            }
        }
    }

    // options are the chart's: proportional, of the total, in units, of its
    // height, of the groups' colours, and their order, without a title, a
    // toolbar, or animations, as it's changed while it's indexed
    private options(): MeterChartOptions {
        return {
            height: MeterHeight,
            animations: false,
            toolbar: { enabled: false },
            accessibility: { svgAriaLabel: this.label },
            color: { scale: { ...this.colors } },
            legend: { order: this.groups.map((g) => g.group), clickable: false },
            meter: { proportional: { total: this.total, unit: this.unit } },
        } as MeterChartOptions;
    }
}

if (!customElements.get(MeterTag)) {
    customElements.define(MeterTag, MeterElement);
}

declare global {
    interface HTMLElementTagNameMap {
        [MeterTag]: MeterElement;
    }
}
