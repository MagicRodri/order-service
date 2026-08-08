package domain

import "testing"

func TestPrice(t *testing.T) {
	items := []Item{
		{SKU: "book", Quantity: 2, UnitPriceCents: 1_500},
		{SKU: "pen", Quantity: 3, UnitPriceCents: 200},
	}

	cases := []struct {
		name                                   string
		discountBps                            int32
		wantSubtotal, wantDiscount, wantTotal  int64
	}{
		{"no discount", 0, 3_600, 0, 3_600},
		{"gold 5%", 500, 3_600, 180, 3_420},
		{"platinum 10%", 1000, 3_600, 360, 3_240},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			subtotal, discount, total := Price(items, tc.discountBps)
			if subtotal != tc.wantSubtotal || discount != tc.wantDiscount || total != tc.wantTotal {
				t.Errorf("Price() = (%d, %d, %d), want (%d, %d, %d)",
					subtotal, discount, total, tc.wantSubtotal, tc.wantDiscount, tc.wantTotal)
			}
		})
	}
}

// Integer division must never let a discount round up past the subtotal.
func TestPriceRoundsDiscountDown(t *testing.T) {
	items := []Item{{SKU: "odd", Quantity: 1, UnitPriceCents: 999}}
	subtotal, discount, total := Price(items, 500)
	if discount != 49 {
		t.Errorf("discount = %d, want 49 (rounded down from 49.95)", discount)
	}
	if subtotal-discount != total {
		t.Errorf("total = %d, want %d", total, subtotal-discount)
	}
}

func TestCustomerViewBlocked(t *testing.T) {
	if !(CustomerView{Status: "BLOCKED"}).Blocked() {
		t.Error("BLOCKED view should report Blocked()")
	}
	if (CustomerView{Status: "ACTIVE"}).Blocked() {
		t.Error("ACTIVE view should not report Blocked()")
	}
}
