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

// MeterHeight is a meter's height, of its title, its total, its bar, and its
// legend, as its chart has none of its own, of a parent of no height
const MeterHeight = '128px';

///////////////////////////////////////////////////////////////////////////////
// ELEMENT

// MeterElement is a proportional meter chart of its groups, of their total,
// in units, such as events, whose chart is made once, and its data and
// options changed, as they are
export class MeterElement extends LitElement {
    static override properties = {
        groups: { attribute: false },
        total: { type: Number },
        unit: { type: String },
        label: { type: String },
    };

    declare groups: readonly MeterGroup[];
    declare total: number;
    declare unit: string;
    declare label: string;
    private chart?: MeterChart;

    constructor() {
        super();
        this.groups = [];
        this.total = 0;
        this.unit = '';
        this.label = '';
    }

    // The chart is in the light DOM, as Carbon Charts' styles are global
    protected override createRenderRoot() {
        return this;
    }

    override disconnectedCallback() {
        super.disconnectedCallback();
        this.chart?.destroy();
        this.chart = undefined;
        this.replaceChildren();
    }

    protected override updated(changed: PropertyValues<this>) {
        const data = this.groups.map((g) => ({ ...g })) as ChartTabularData;
        const options = this.options();
        if (!this.chart) {
            const holder = document.createElement('div');
            this.replaceChildren(holder);
            this.chart = new MeterChart(holder, { data, options });
            return;
        }
        if (changed.has('groups')) {
            this.chart.model.setData(data);
        }
        if (changed.has('total') || changed.has('unit') || changed.has('label')) {
            this.chart.model.setOptions(options);
        }
    }

    // options are the chart's: proportional, of the total, in units, of its
    // height, without a toolbar, or animations, as it's changed while it's
    // indexed
    private options(): MeterChartOptions {
        return {
            title: this.label,
            height: MeterHeight,
            animations: false,
            toolbar: { enabled: false },
            accessibility: { svgAriaLabel: this.label },
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
