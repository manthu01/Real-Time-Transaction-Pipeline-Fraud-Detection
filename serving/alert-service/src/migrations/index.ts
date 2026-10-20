import { BaselineAlertsSchema1789516800000 } from './1789516800000-BaselineAlertsSchema';
import { AddShapExplanation1792454400000 } from './1792454400000-AddShapExplanation';

/** Listed explicitly (not globbed from dist/) so the same list works under
 * ts-node, jest, and the compiled service alike. Order matters. */
export const MIGRATIONS = [BaselineAlertsSchema1789516800000, AddShapExplanation1792454400000];
